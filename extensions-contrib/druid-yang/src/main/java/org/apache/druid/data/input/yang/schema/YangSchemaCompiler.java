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
import org.yangcentral.yangkit.base.YangElement;
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
import org.yangcentral.yangkit.model.api.stmt.Case;
import org.yangcentral.yangkit.model.api.stmt.Choice;
import org.yangcentral.yangkit.model.api.stmt.Container;
import org.yangcentral.yangkit.model.api.stmt.Leaf;
import org.yangcentral.yangkit.model.api.stmt.LeafList;
import org.yangcentral.yangkit.model.api.stmt.Module;
import org.yangcentral.yangkit.model.api.stmt.Notification;
import org.yangcentral.yangkit.model.api.stmt.SchemaNode;
import org.yangcentral.yangkit.model.api.stmt.SchemaNodeContainer;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Compiles YANG module sources into a {@link YangSchemaIndex} using yangkit, the parser used by the network-analytics
 * YANG schema registry plugin.
 */
public class YangSchemaCompiler
{
  private static final int MAX_DEPTH = 64;
  private static final Pattern COUNTER_OR_GAUGE = Pattern.compile(".*(counter|gauge).*");
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
      for (Map.Entry<String, String> source : sources.entrySet()) {
        parse(source.getKey(), source.getValue(), context);
      }
      final ValidatorResult result = context.validate();
      if (!result.isOk()) {
        throw new YangSchemaException("Invalid YANG schema from %s: %s", sources.keySet(), result);
      }

      final YangSchemaIndex.Node root =
          new YangSchemaIndex.Node("", null, YangSchemaIndex.Kind.ROOT, null, ImmutableList.of());
      // sx:structure definitions (RFC 8791) are not instance data; the tree view presents them as containers, so
      // identify them in the schema view.
      final Set<String> structures = new HashSet<>();
      for (SchemaNode node : context.getSchemaNodeChildren()) {
        if (node instanceof YangStructure) {
          structures.add(node.getIdentifier().getLocalName());
        }
      }
      addChildren(context, root, structures, 0);
      final List<String> modules = new ArrayList<>();
      for (Module module : context.getModules()) {
        modules.add(module.getArgStr());
      }
      return new YangSchemaIndex(root, modules);
    }
  }

  private static void parse(String name, String text, YangSchemaContext context)
  {
    final YangParserEnv env = new YangParserEnv();
    env.setYangStr(text);
    env.setFilename(name);
    env.setCurPos(0);
    final List<YangElement> elements;
    try {
      elements = new YangParser().parseYang(text, env);
    }
    catch (YangParserException e) {
      throw new YangSchemaException(e, "Cannot parse YANG module [%s]", name);
    }
    for (YangElement element : elements) {
      if (element instanceof Module) {
        context.addModule((Module) element);
      }
    }
    context.getParseResult().put(name, elements);
  }

  /**
   * Adds the instance-data children of {@code container} to {@code parent}, walking yangkit's tree view (augments and
   * groupings resolved). yangkit's data-node view is not used because it merges the contents of notifications into
   * their parent. {@code choice} and {@code case} are transparent, as in the RFC 7951 encoding. Notifications,
   * including YANG 1.1 notifications nested in containers and lists, become named containers, since notification
   * messages carry them by name. RPCs, actions, top-level {@code sx:structure} definitions, {@code anydata} and
   * {@code anyxml} are skipped: they are not instance data or have no schema below them.
   */
  private static void addChildren(
      SchemaNodeContainer container,
      YangSchemaIndex.Node parent,
      Set<String> skipNames,
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
        addChildren((SchemaNodeContainer) schemaNode, parent, skipNames, depth + 1);
        continue;
      }
      final YangSchemaIndex.Node node;
      if (schemaNode instanceof Leaf) {
        node = new YangSchemaIndex.Node(
            name(schemaNode),
            module(schemaNode),
            YangSchemaIndex.Kind.LEAF,
            valueType(((TypedDataNode) schemaNode).getType()),
            ImmutableList.of()
        );
      } else if (schemaNode instanceof LeafList) {
        node = new YangSchemaIndex.Node(
            name(schemaNode),
            module(schemaNode),
            YangSchemaIndex.Kind.LEAF_LIST,
            valueType(((TypedDataNode) schemaNode).getType()),
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
        // rpc, action, input, output, sx:structure, anydata, anyxml
        continue;
      }
      parent.addChild(node);
      if (schemaNode instanceof SchemaNodeContainer) {
        addChildren((SchemaNodeContainer) schemaNode, node, Set.of(), depth + 1);
      }
    }
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
