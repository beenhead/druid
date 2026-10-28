/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.yang.schema;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.yangcentral.yangkit.base.Position;
import org.yangcentral.yangkit.base.YangElement;
import org.yangcentral.yangkit.common.api.exception.Severity;
import org.yangcentral.yangkit.common.api.validate.ValidatorRecord;
import org.yangcentral.yangkit.common.api.validate.ValidatorResult;
import org.yangcentral.yangkit.model.api.restriction.Decimal64;
import org.yangcentral.yangkit.model.api.restriction.Empty;
import org.yangcentral.yangkit.model.api.restriction.LeafRef;
import org.yangcentral.yangkit.model.api.restriction.Restriction;
import org.yangcentral.yangkit.model.api.restriction.UInt64;
import org.yangcentral.yangkit.model.api.restriction.Union;
import org.yangcentral.yangkit.model.api.restriction.YangBoolean;
import org.yangcentral.yangkit.model.api.restriction.YangInteger;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.model.api.stmt.Anydata;
import org.yangcentral.yangkit.model.api.stmt.Case;
import org.yangcentral.yangkit.model.api.stmt.Choice;
import org.yangcentral.yangkit.model.api.stmt.Container;
import org.yangcentral.yangkit.model.api.stmt.DataDefinition;
import org.yangcentral.yangkit.model.api.stmt.Identity;
import org.yangcentral.yangkit.model.api.stmt.Leaf;
import org.yangcentral.yangkit.model.api.stmt.LeafList;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.model.api.stmt.Notification;
import org.yangcentral.yangkit.model.api.stmt.SchemaNode;
import org.yangcentral.yangkit.model.api.stmt.SchemaNodeContainer;
import org.yangcentral.yangkit.model.api.stmt.SubModule;
import org.yangcentral.yangkit.model.api.stmt.Type;
import org.yangcentral.yangkit.model.api.stmt.TypedDataNode;
import org.yangcentral.yangkit.model.api.stmt.YangList;
import org.yangcentral.yangkit.model.api.stmt.ext.YangStructure;
import org.yangcentral.yangkit.parser.YangParser;
import org.yangcentral.yangkit.parser.YangParserEnv;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.register.YangStatementImplRegister;
import org.yangcentral.yangkit.register.YangStatementRegister;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compiles YANG module sources into a {@link YangSchemaIndex} using yangkit, the parser used by the network-analytics
 * YANG schema registry plugin.
 *
 * <p>Compilation is tolerant: a device's YANG library commonly contains a few modules that yangkit rejects, usually
 * ones unrelated to what a subscription sends. Modules that fail to parse or validate, and modules depending on them,
 * are recorded in {@link YangSchemaIndex#getBrokenModules()} and logged; the rest of the schema is used. Only a schema
 * in which every module is broken is rejected.
 */
public class YangSchemaCompiler
{
  private static final Logger log = new Logger(YangSchemaCompiler.class);
  private static final int MAX_DEPTH = 64;
  private static final int MAX_LOGGED_MODULES = 20;
  private static final Pattern COUNTER_OR_GAUGE = Pattern.compile(".*(counter|gauge).*");
  private static final Pattern ERROR_SOURCE = Pattern.compile("source:([^\\s\\[]+)");
  private static boolean registered = false;

  private YangSchemaCompiler()
  {
  }

  /**
   * @param sources module sources keyed by a name used in error messages (e.g. file name or registry subject). All
   *                imported modules must be included.
   */
  public static YangSchemaIndex compile(Map<String, String> sources)
  {
    // yangkit keeps statement registrations in static state; parsing is serialized to be safe.
    synchronized (YangSchemaCompiler.class) {
      if (!registered) {
        YangStatementImplRegister.registerImpl();
        registered = true;
      }
      final YangSchemaContext context = YangStatementRegister.getInstance().getSchemeContextInstance();
      // source name, module or submodule name -> the module it belongs to; complete once validation has resolved
      // belongs-to
      final Map<String, String> owners = new HashMap<>();
      final Map<String, List<Module>> modulesBySource = new HashMap<>();
      final Map<String, String> broken = new LinkedHashMap<>();
      for (Map.Entry<String, String> source : sources.entrySet()) {
        parse(source.getKey(), source.getValue(), context, modulesBySource, owners, broken);
      }
      final ValidatorResult result = context.validate();
      for (Map.Entry<String, List<Module>> entry : modulesBySource.entrySet()) {
        for (Module module : entry.getValue()) {
          owners.put(module.getArgStr(), owner(module));
          owners.putIfAbsent(entry.getKey(), owner(module));
        }
      }
      if (!result.isOk()) {
        for (ValidatorRecord<?, ?> record : result.getRecords()) {
          if (record.getSeverity() == Severity.ERROR) {
            final String source = errorSource(record);
            final String owner = owners.containsKey(source)
                                 ? owners.get(source)
                                 : owners.getOrDefault(baseName(source), baseName(source));
            broken.putIfAbsent(owner, describe(record));
          }
        }
      }
      final Set<String> owned = new HashSet<>(owners.values());
      if (owned.isEmpty() || broken.keySet().containsAll(owned)) {
        throw new YangSchemaException(
            "Invalid YANG schema from %s: %s",
            sources.keySet(),
            result.isOk() ? broken : result
        );
      }
      if (!broken.isEmpty()) {
        log.warn(
            "YANG schema from %d sources compiled with %d broken modules, which are ignored: %s",
            sources.size(),
            broken.size(),
            limit(broken)
        );
      }

      final YangValueChecks checks = new YangValueChecks(identities(context));
      final YangSchemaIndex.Node root =
          new YangSchemaIndex.Node("", null, YangSchemaIndex.Kind.ROOT, null, ImmutableList.of());
      final YangSchemaIndex.Node structureRoot =
          new YangSchemaIndex.Node("", null, YangSchemaIndex.Kind.ROOT, null, ImmutableList.of());
      // sx:structure definitions (RFC 8791) are not instance data; the tree view presents them as containers, so
      // identify them in the schema view and index them separately.
      final Set<String> structures = new HashSet<>();
      for (SchemaNode node : context.getSchemaNodeChildren()) {
        if (node instanceof YangStructure) {
          structures.add(node.getIdentifier().getLocalName());
          final YangSchemaIndex.Node structure = new YangSchemaIndex.Node(
              name(node),
              module(node),
              YangSchemaIndex.Kind.CONTAINER,
              null,
              ImmutableList.of()
          );
          structureRoot.addChild(structure);
          addChildren((SchemaNodeContainer) node, structure, Set.of(), checks, false, 0);
        }
      }
      addChildren(context, root, structures, checks, false, 0);
      final Set<String> notifications = new HashSet<>();
      for (SchemaNode node : context.getSchemaNodeChildren()) {
        if (node instanceof Notification) {
          notifications.add(module(node) + ":" + name(node));
        }
      }
      final List<String> modules = new ArrayList<>();
      for (Module module : context.getModules()) {
        modules.add(module.getArgStr());
      }
      return new YangSchemaIndex(root, structureRoot, notifications, modules, broken);
    }
  }

  private static void parse(
      String name,
      String text,
      YangSchemaContext context,
      Map<String, List<Module>> modulesBySource,
      Map<String, String> owners,
      Map<String, String> broken
  )
  {
    final YangParserEnv env = new YangParserEnv();
    env.setYangStr(text);
    env.setFilename(name);
    env.setCurPos(0);
    final List<YangElement> elements;
    try {
      elements = new YangParser().parseYang(text, env);
    }
    catch (YangParserException | RuntimeException e) {
      final String module = baseName(name);
      owners.put(module, module);
      broken.putIfAbsent(module, StringUtils.format("cannot parse [%s]: %s", name, e.getMessage()));
      return;
    }
    for (YangElement element : elements) {
      if (element instanceof Module) {
        final Module module = (Module) element;
        context.addModule(module);
        modulesBySource.computeIfAbsent(name, k -> new ArrayList<>()).add(module);
      }
    }
    context.getParseResult().put(name, elements);
  }

  private static String owner(Module module)
  {
    if (module instanceof SubModule && ((SubModule) module).getBelongsto() != null) {
      return ((SubModule) module).getBelongsto().getArgStr();
    }
    return module.getArgStr();
  }

  /**
   * The source a validation record refers to: a file name or registry subject, sometimes followed by a revision.
   */
  private static String errorSource(ValidatorRecord<?, ?> record)
  {
    final Object path = record.getErrorPath();
    if (path instanceof Position && ((Position) path).getSource() != null) {
      return ((Position) path).getSource();
    }
    final Matcher matcher = ERROR_SOURCE.matcher(String.valueOf(path));
    return matcher.find() ? matcher.group(1) : String.valueOf(path);
  }

  /**
   * {@code ietf-interfaces@2018-02-20.yang} -> {@code ietf-interfaces}
   */
  private static String baseName(String source)
  {
    String name = source;
    if (name.endsWith(".yang")) {
      name = name.substring(0, name.length() - ".yang".length());
    }
    final int at = name.indexOf('@');
    return at < 0 ? name : name.substring(0, at);
  }

  private static String describe(ValidatorRecord<?, ?> record)
  {
    final String message = record.getErrorMsg() == null ? null : record.getErrorMsg().getMessage();
    return StringUtils.format("%s: %s", record.getErrorPath(), message == null ? record.getErrorTag() : message);
  }

  private static Map<String, String> limit(Map<String, String> broken)
  {
    final Map<String, String> limited = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : broken.entrySet()) {
      if (limited.size() == MAX_LOGGED_MODULES) {
        limited.put("...", StringUtils.format("%d more", broken.size() - MAX_LOGGED_MODULES));
        break;
      }
      limited.put(entry.getKey(), entry.getValue());
    }
    return limited;
  }

  private static Map<String, Identity> identities(YangSchemaContext context)
  {
    final Map<String, Identity> identities = new HashMap<>();
    for (Module module : context.getModules()) {
      if (module.getIdentities() == null) {
        continue;
      }
      for (Identity identity : module.getIdentities()) {
        identities.put(owner(module) + ":" + identity.getArgStr(), identity);
      }
    }
    return identities;
  }

  /**
   * Adds the instance-data children of {@code container} to {@code parent}, walking yangkit's tree view (augments and
   * groupings resolved). yangkit's data-node view is not used because it merges the contents of notifications into
   * their parent. {@code choice} and {@code case} are transparent, as in the RFC 7951 encoding. Notifications,
   * including YANG 1.1 notifications nested in containers and lists, become named containers, since notification
   * messages carry them by name. RPCs, actions, top-level {@code sx:structure} definitions and {@code anyxml} are
   * skipped: they are not instance data or have no schema below them.
   *
   * @param inChoice whether {@code container} is a choice or case, or inside one below {@code parent}; nodes there are
   *                 never mandatory, since another case may have been chosen
   */
  private static void addChildren(
      SchemaNodeContainer container,
      YangSchemaIndex.Node parent,
      Set<String> skipNames,
      YangValueChecks checks,
      boolean inChoice,
      int depth
  )
  {
    if (depth > MAX_DEPTH) {
      throw new YangSchemaException("YANG schema tree deeper than %d levels", MAX_DEPTH);
    }
    for (SchemaNode schemaNode : container.getTreeNodeChildren()) {
      if (skipNames.contains(schemaNode.getIdentifier().getLocalName())) {
        continue;
      }
      if (schemaNode instanceof Choice || schemaNode instanceof Case) {
        addChildren((SchemaNodeContainer) schemaNode, parent, skipNames, checks, true, depth + 1);
        continue;
      }
      final YangSchemaIndex.Node node;
      if (schemaNode instanceof Leaf) {
        final Leaf leaf = (Leaf) schemaNode;
        node = new YangSchemaIndex.Node(
            name(schemaNode),
            module(schemaNode),
            YangSchemaIndex.Kind.LEAF,
            valueType(leaf.getType()),
            ImmutableList.of(),
            !inChoice && isMandatory(leaf, parent),
            checks.forType(leaf.getType())
        );
      } else if (schemaNode instanceof LeafList) {
        node = new YangSchemaIndex.Node(
            name(schemaNode),
            module(schemaNode),
            YangSchemaIndex.Kind.LEAF_LIST,
            valueType(((TypedDataNode) schemaNode).getType()),
            ImmutableList.of(),
            false,
            checks.forType(((TypedDataNode) schemaNode).getType())
        );
      } else if (schemaNode instanceof Anydata) {
        node = new YangSchemaIndex.Node(
            name(schemaNode),
            module(schemaNode),
            YangSchemaIndex.Kind.ANYDATA,
            null,
            ImmutableList.of()
        );
      } else if (schemaNode instanceof YangList) {
        final YangList list = (YangList) schemaNode;
        final List<String> keys = new ArrayList<>();
        if (list.getKey() != null) {
          for (Leaf key : list.getKey().getkeyNodes()) {
            keys.add(key.getIdentifier().getLocalName());
          }
        }
        node = new YangSchemaIndex.Node(name(schemaNode), module(schemaNode), YangSchemaIndex.Kind.LIST, null, keys);
      } else if (schemaNode instanceof Container || schemaNode instanceof Notification) {
        node = new YangSchemaIndex.Node(
            name(schemaNode),
            module(schemaNode),
            YangSchemaIndex.Kind.CONTAINER,
            null,
            ImmutableList.of()
        );
      } else {
        // rpc, action, input, output, sx:structure, anyxml
        continue;
      }
      parent.addChild(node);
      if (schemaNode instanceof SchemaNodeContainer) {
        addChildren((SchemaNodeContainer) schemaNode, node, Set.of(), checks, false, depth + 1);
      }
    }
  }

  /**
   * A leaf is enforced as mandatory only if it is unconditionally so: declared {@code mandatory true}, without
   * {@code when} or {@code if-feature}, not in a choice, and from the same module as its parent. Leaves added by an
   * augment are skipped, since RFC 7950 section 7.17 only allows them to be mandatory under a condition on the augment.
   */
  private static boolean isMandatory(Leaf leaf, YangSchemaIndex.Node parent)
  {
    // The statement's argument rather than Mandatory.getValue(), which yangkit leaves false inside sx:structure.
    if (leaf.getMandatory() == null || !"true".equals(leaf.getMandatory().getArgStr())) {
      return false;
    }
    final DataDefinition definition = leaf;
    if (definition.getWhen() != null
        || (definition.getIfFeatures() != null && !definition.getIfFeatures().isEmpty())) {
      return false;
    }
    // The tree view doesn't always show choices (e.g. inside notifications), so look at the leaf's own ancestry too.
    for (SchemaNodeContainer ancestor = leaf.getParentSchemaNode();
         ancestor instanceof SchemaNode && !(ancestor instanceof Module);
         ancestor = ((SchemaNode) ancestor).getParentSchemaNode()) {
      if (ancestor instanceof Choice || ancestor instanceof Case) {
        return false;
      }
      if (ancestor instanceof Container || ancestor instanceof YangList || ancestor instanceof Notification) {
        break;
      }
    }
    return parent.getModule() == null || parent.getModule().equals(module(leaf));
  }

  private static String name(SchemaNode node)
  {
    return node.getIdentifier().getLocalName();
  }

  @Nullable
  private static String module(SchemaNode node)
  {
    return moduleName(node.getJsonIdentifier());
  }

  static YangSchemaIndex.ValueType valueType(@Nullable Type type)
  {
    return valueType(type, 0);
  }

  private static YangSchemaIndex.ValueType valueType(@Nullable Type type, int leafrefDepth)
  {
    if (type == null) {
      return YangSchemaIndex.ValueType.OTHER;
    }
    final Restriction<?> restriction = type.getRestriction();
    if (restriction instanceof LeafRef) {
      // A leafref has the type of the leaf it points to (e.g. ietf-incident's incident-ref -> incident-no, a uint64).
      final TypedDataNode target = ((LeafRef) restriction).getReferencedNode();
      return target == null || leafrefDepth > 8 ? YangSchemaIndex.ValueType.OTHER : valueType(target.getType(), leafrefDepth + 1);
    }
    if (restriction instanceof UInt64 && !isCounterOrGauge(type)) {
      return YangSchemaIndex.ValueType.INTEGER_EXACT;
    } else if (restriction instanceof YangInteger) {
      return YangSchemaIndex.ValueType.INTEGER;
    } else if (restriction instanceof Decimal64) {
      return YangSchemaIndex.ValueType.DECIMAL;
    } else if (restriction instanceof YangBoolean) {
      return YangSchemaIndex.ValueType.BOOLEAN;
    } else if (restriction instanceof Empty) {
      return YangSchemaIndex.ValueType.EMPTY;
    } else if (restriction instanceof Union) {
      // RFC 7950 section 9.12: a value is matched against the member types in order. We only resolve the common
      // case where the first member is numeric; anything else is left as encoded.
      final List<Type> members = ((Union) restriction).getActualTypes();
      if (members != null && !members.isEmpty()) {
        final YangSchemaIndex.ValueType first = valueType(members.get(0), leafrefDepth);
        if (first == YangSchemaIndex.ValueType.INTEGER
            || first == YangSchemaIndex.ValueType.INTEGER_EXACT
            || first == YangSchemaIndex.ValueType.DECIMAL) {
          return first;
        }
      }
      return YangSchemaIndex.ValueType.OTHER;
    }
    return YangSchemaIndex.ValueType.OTHER;
  }

  /**
   * Whether the type is derived, at any level, from a typedef named like a counter or gauge ({@code yang:counter64},
   * {@code yang:gauge64}, {@code yang:zero-based-counter64}, vendor equivalents).
   */
  private static boolean isCounterOrGauge(Type type)
  {
    Type current = type;
    for (int i = 0; current != null && current.isDerivedType() && current.getDerived() != null && i < 16; i++) {
      final String name = current.getDerived().getArgStr();
      if (name != null && COUNTER_OR_GAUGE.matcher(name).matches()) {
        return true;
      }
      current = current.getDerived().getType();
    }
    return false;
  }

  @Nullable
  private static String moduleName(@Nullable String jsonIdentifier)
  {
    if (jsonIdentifier == null) {
      return null;
    }
    final int colon = jsonIdentifier.indexOf(':');
    return colon < 0 ? null : jsonIdentifier.substring(0, colon);
  }

  public static class YangSchemaException extends RuntimeException
  {
    public YangSchemaException(String format, Object... args)
    {
      super(StringUtils.nonStrictFormat(format, args));
    }

    public YangSchemaException(Throwable cause, String format, Object... args)
    {
      super(StringUtils.nonStrictFormat(format, args), cause);
    }
  }
}
