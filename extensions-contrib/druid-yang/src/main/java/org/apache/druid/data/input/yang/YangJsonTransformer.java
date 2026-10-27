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

package org.apache.druid.data.input.yang;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.yang.schema.YangSchemaIndex;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Turns one YANG instance-data document encoded as JSON per RFC 7951 into zero or more row objects.
 *
 * <ol>
 *   <li>Optionally strips module prefixes from member names ({@code "ietf-interfaces:interfaces"} becomes
 *   {@code "interfaces"}).</li>
 *   <li>Types values. With a {@link YangSchemaIndex}, each leaf is typed by its YANG built-in type: integers and
 *   decimal64 become numbers (RFC 7951 encodes 64-bit integers and decimal64 as strings), {@code empty} ({@code [null]})
 *   becomes {@code true}, and everything else is left as encoded. Without a schema, or for nodes the schema doesn't
 *   cover, {@code coerceTypes} applies a heuristic instead.</li>
 *   <li>Optionally unwraps notification envelopes (RFC 8040 / RFC 8641 YANG-Push, and the notification envelope of
 *   draft-ietf-netconf-notif-envelope, and the telemetry message of draft-ietf-nmop-message-broker-telemetry-message
 *   whose metadata is flattened), lifting their scalar fields onto every row. The event time is always
 *   written as {@link #EVENT_TIME}. For {@code push-change-update}, each YANG-Patch edit is rebuilt into a data tree
 *   from its target and value, and tagged with {@link #YANG_OPERATION} and {@link #YANG_TARGET}.</li>
 *   <li>Applies each {@link YangExplodeRule}: every entry of the list at the rule's path becomes one row, tagged with
 *   {@link #YANG_RULE}. Scalar leaves of enclosing list entries (typically the list keys) are copied onto each row as
 *   {@code <node>.<leaf>}, as are those of enclosing containers. With no rules, the whole data tree is one row.</li>
 * </ol>
 */
public class YangJsonTransformer
{
  public static final String YANG_RULE = "yang-rule";
  public static final String YANG_UPDATE = "yang-update";
  public static final String YANG_OPERATION = "yang-operation";
  public static final String YANG_TARGET = "yang-target";
  public static final String EVENT_TIME = "eventTime";
  static final String SUBSCRIPTION_ID = "subscription-id";
  static final String ANCESTOR_SEPARATOR = ".";
  /**
   * Prefix for an envelope field whose name is also used by the row's data, e.g. {@code envelope.hostname} when an
   * embedded notification has its own {@code hostname}.
   */
  public static final String ENVELOPE_PREFIX = "envelope.";

  /**
   * Local names of envelope and YANG-Push nodes that are unwrapped when {@code unwrapEnvelope} is set.
   */
  private static final Set<String> ENVELOPE_NODES = ImmutableSet.of(
      "notification",
      "envelope",
      "contents",
      "notification-contents",
      "push-update",
      "push-change-update",
      "datastore-contents"
  );
  private static final Set<String> EVENT_TIME_NAMES = ImmutableSet.of("eventTime", "event-time");
  /**
   * draft-ietf-nmop-message-broker-telemetry-message: a collector wraps each notification in a {@code message}
   * structure holding metadata containers and the notification itself as {@code payload}.
   */
  private static final String TELEMETRY_MESSAGE = "message";
  private static final String TELEMETRY_PAYLOAD = "payload";
  private static final String TELEMETRY_METADATA = "telemetry-message-metadata";
  private static final String TELEMETRY_LABELS = "labels";
  private static final List<String> TELEMETRY_LABEL_VALUES = ImmutableList.of("number-value", "string-value", "anydata-values");
  /**
   * Fallbacks for {@link #EVENT_TIME}, in order, when the payload carries no notification time.
   */
  private static final List<String> TELEMETRY_EVENT_TIMES = ImmutableList.of(
      TELEMETRY_METADATA + ANCESTOR_SEPARATOR + "node-export-timestamp",
      TELEMETRY_METADATA + ANCESTOR_SEPARATOR + "collection-timestamp"
  );
  private static final String PUSH_UPDATE = "push-update";
  private static final String PUSH_CHANGE_UPDATE = "push-change-update";
  private static final String ID = "id";

  // No leading zeros, so identifiers such as "007" stay strings.
  private static final Pattern INTEGER = Pattern.compile("-?(0|[1-9][0-9]*)");
  private static final Pattern DECIMAL = Pattern.compile("-?(0|[1-9][0-9]*)\\.[0-9]+");

  private final JsonNodeFactory factory = JsonNodeFactory.instance;
  private final List<YangExplodeRule> rules;
  private final boolean stripModulePrefixes;
  private final boolean coerceTypes;
  private final boolean unwrapEnvelope;
  private final boolean emitUnmatched;

  public YangJsonTransformer(
      List<YangExplodeRule> rules,
      boolean stripModulePrefixes,
      boolean coerceTypes,
      boolean unwrapEnvelope,
      boolean emitUnmatched
  )
  {
    this.rules = ImmutableList.copyOf(rules);
    this.stripModulePrefixes = stripModulePrefixes;
    this.coerceTypes = coerceTypes;
    this.unwrapEnvelope = unwrapEnvelope;
    this.emitUnmatched = emitUnmatched;
  }

  /**
   * Returns the rows for one document. The list is empty only if the document matched no rule and
   * {@code emitUnmatched} is off, or if the document is not a JSON object.
   */
  public List<ObjectNode> transform(JsonNode document)
  {
    return transform(document, null);
  }

  /**
   * @param schema the message's YANG schema, or null to type values heuristically
   */
  public List<ObjectNode> transform(JsonNode document, @Nullable YangSchemaIndex schema)
  {
    final JsonNode normalized = normalize(document, coerceTypes && schema == null);
    if (!normalized.isObject()) {
      return Collections.emptyList();
    }

    final ObjectNode envelope = factory.objectNode();
    final List<DataTree> trees = new ArrayList<>();
    if (unwrapEnvelope) {
      unwrap((ObjectNode) normalized, envelope, trees, schema);
    } else {
      trees.add(new DataTree((ObjectNode) normalized, factory.objectNode(), null));
    }
    if (schema != null) {
      for (DataTree tree : trees) {
        typeChildren(tree.data, schema.getRoot());
      }
    }

    final List<ObjectNode> rows = new ArrayList<>();
    for (DataTree tree : trees) {
      final ObjectNode base = envelope.deepCopy();
      base.setAll(tree.extras);
      if (rules.isEmpty()) {
        rows.add(buildRow(base, factory.objectNode(), tree.data));
        continue;
      }
      for (YangExplodeRule rule : rules) {
        final ObjectNode ruleBase = base.deepCopy();
        ruleBase.put(YANG_RULE, rule.getName());
        final int before = rows.size();
        explode(tree.data, rule.getSegments(), 0, ruleBase, factory.objectNode(), rows);
        if (rows.size() == before && tree.valuelessTarget != null && related(tree.valuelessTarget, rule)) {
          // A delete/remove edit carries no value, but is still an event for this rule.
          rows.add(ruleBase);
        }
      }
    }

    if (rows.isEmpty() && emitUnmatched) {
      rows.add(envelope);
    }
    return rows;
  }

  /**
   * Recursively rebuilds the tree, stripping module prefixes and coercing values as configured.
   */
  private JsonNode normalize(JsonNode node, boolean coerce)
  {
    if (node.isObject()) {
      final ObjectNode out = factory.objectNode();
      final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        final Map.Entry<String, JsonNode> field = fields.next();
        String name = stripModulePrefixes ? localName(field.getKey()) : field.getKey();
        if (out.has(name)) {
          // Two modules contributed a node with the same local name (e.g. augmentations); keep the qualified name.
          name = field.getKey();
        }
        out.set(name, normalize(field.getValue(), coerce));
      }
      return out;
    } else if (node.isArray()) {
      if (coerce && isEmptyValue(node)) {
        // RFC 7951 section 6.9: a leaf of type "empty" is encoded as [null].
        return BooleanNode.TRUE;
      }
      final ArrayNode out = factory.arrayNode(node.size());
      for (JsonNode element : node) {
        out.add(normalize(element, coerce));
      }
      return out;
    } else if (coerce && node.isTextual()) {
      return coerceText(node);
    } else {
      return node;
    }
  }

  /**
   * RFC 7951 section 6.1: int64, uint64 and decimal64 values are encoded as JSON strings.
   */
  private JsonNode coerceText(JsonNode node)
  {
    final String text = node.textValue();
    if (text.length() > 40) {
      return node;
    }
    if (INTEGER.matcher(text).matches()) {
      try {
        return LongNode.valueOf(Long.parseLong(text));
      }
      catch (NumberFormatException e) {
        // uint64 beyond Long.MAX_VALUE; fall back to a (lossy) double.
        return DoubleNode.valueOf(new BigDecimal(text).doubleValue());
      }
    }
    if (DECIMAL.matcher(text).matches()) {
      return DoubleNode.valueOf(Double.parseDouble(text));
    }
    return node;
  }

  /**
   * Types the members of {@code object} according to {@code schemaNode}, a container, list entry or the root.
   */
  private void typeChildren(ObjectNode object, YangSchemaIndex.Node schemaNode)
  {
    final Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      final YangSchemaIndex.Node child = schemaNode.child(field.getKey());
      if (child == null) {
        if (coerceTypes) {
          field.setValue(normalize(field.getValue(), true));
        }
        continue;
      }
      field.setValue(typeValue(field.getValue(), child));
    }
  }

  private JsonNode typeValue(JsonNode value, YangSchemaIndex.Node node)
  {
    switch (node.getKind()) {
      case CONTAINER:
        if (value.isObject()) {
          typeChildren((ObjectNode) value, node);
        }
        return value;
      case LIST:
        if (value.isArray()) {
          for (JsonNode entry : value) {
            if (entry.isObject()) {
              typeChildren((ObjectNode) entry, node);
            }
          }
        } else if (value.isObject()) {
          typeChildren((ObjectNode) value, node);
        }
        return value;
      case LEAF_LIST:
        if (value.isArray()) {
          final ArrayNode out = factory.arrayNode(value.size());
          for (JsonNode element : value) {
            out.add(typeLeaf(element, node.getValueType()));
          }
          return out;
        }
        return typeLeaf(value, node.getValueType());
      case LEAF:
        return typeLeaf(value, node.getValueType());
      default:
        return value;
    }
  }

  private JsonNode typeLeaf(JsonNode value, @Nullable YangSchemaIndex.ValueType type)
  {
    if (type == null) {
      return value;
    }
    switch (type) {
      case INTEGER:
      case DECIMAL:
        // Strings for 64-bit values (RFC 7951 section 6.1) and for list keys taken from instance identifiers.
        return value.isTextual() ? coerceText(value) : value;
      case INTEGER_EXACT:
        if (value.isTextual() && INTEGER.matcher(value.textValue()).matches()) {
          try {
            return LongNode.valueOf(Long.parseLong(value.textValue()));
          }
          catch (NumberFormatException e) {
            // Beyond the long range: keep the exact digits rather than a lossy double.
            return value;
          }
        }
        return value;
      case BOOLEAN:
        if (value.isTextual() && ("true".equals(value.textValue()) || "false".equals(value.textValue()))) {
          return BooleanNode.valueOf("true".equals(value.textValue()));
        }
        return value;
      case EMPTY:
        return isEmptyValue(value) ? BooleanNode.TRUE : value;
      default:
        return value;
    }
  }

  private static boolean isEmptyValue(JsonNode node)
  {
    return node.isArray() && node.size() == 1 && node.get(0).isNull();
  }

  /**
   * Descends through envelope nodes, copying scalar members of each level into {@code envelope}, and adds the data
   * tree(s) found inside to {@code trees}: one for a push-update or bare document, one per edit for a
   * push-change-update.
   */
  private void unwrap(ObjectNode node, ObjectNode envelope, List<DataTree> trees, @Nullable YangSchemaIndex schema)
  {
    ObjectNode current = node;
    String currentName = null;
    while (true) {
      String nextName = null;
      ObjectNode next = null;
      final Iterator<Map.Entry<String, JsonNode>> fields = current.fields();
      while (fields.hasNext()) {
        final Map.Entry<String, JsonNode> field = fields.next();
        if (field.getValue().isObject() && isEnvelopeNode(currentName, localName(field.getKey()), field.getValue())) {
          nextName = localName(field.getKey());
          next = (ObjectNode) field.getValue();
          break;
        }
      }
      if (next == null) {
        break;
      }
      if (TELEMETRY_MESSAGE.equals(currentName)) {
        copyTelemetryMetadata(current, envelope);
      } else {
        copyScalars(current, envelope, currentName);
      }
      current = next;
      currentName = nextName;
    }
    if (!envelope.has(EVENT_TIME)) {
      // A telemetry message's payload may be a bare push-update, without a notification time.
      for (String name : TELEMETRY_EVENT_TIMES) {
        if (envelope.hasNonNull(name)) {
          envelope.set(EVENT_TIME, envelope.get(name));
          break;
        }
      }
    }

    if (PUSH_CHANGE_UPDATE.equals(currentName)) {
      envelope.put(YANG_UPDATE, PUSH_CHANGE_UPDATE);
      copyScalars(current, envelope, currentName);
      addPatchEdits(current, envelope, trees, schema);
    } else {
      if (PUSH_UPDATE.equals(currentName) || "datastore-contents".equals(currentName)) {
        envelope.put(YANG_UPDATE, PUSH_UPDATE);
      }
      trees.add(new DataTree(current, factory.objectNode(), null));
    }
  }

  /**
   * RFC 8641 section 3.7 / RFC 8072: {@code datastore-changes/yang-patch/edit[]}, each with an operation, a target
   * (a RESTCONF-style instance identifier such as {@code /ietf-interfaces:interfaces/interface=eth0}) and, except for
   * delete and remove, a value whose top-level member is the target node.
   */
  private void addPatchEdits(
      ObjectNode pushChangeUpdate,
      ObjectNode envelope,
      List<DataTree> trees,
      @Nullable YangSchemaIndex schema
  )
  {
    final JsonNode changes = getByLocalName(pushChangeUpdate, "datastore-changes");
    final JsonNode patch = changes != null && changes.isObject() ? getByLocalName((ObjectNode) changes, "yang-patch") : null;
    if (patch == null || !patch.isObject()) {
      return;
    }
    copyScalars((ObjectNode) patch, envelope, null);
    final JsonNode edits = getByLocalName((ObjectNode) patch, "edit");
    if (edits == null || !edits.isArray()) {
      return;
    }
    for (JsonNode edit : edits) {
      if (!edit.isObject()) {
        continue;
      }
      final JsonNode target = getByLocalName((ObjectNode) edit, "target");
      final JsonNode operation = getByLocalName((ObjectNode) edit, "operation");
      final JsonNode value = getByLocalName((ObjectNode) edit, "value");
      if (target == null || !target.isTextual()) {
        continue;
      }
      final List<String> targetPath = targetPath(target.textValue());

      final ObjectNode extras = factory.objectNode();
      extras.set(YANG_TARGET, target);
      if (operation != null) {
        extras.set(YANG_OPERATION, operation);
      }

      // Rebuild the ancestors of the target; list entries become single-element arrays. With a schema, their key
      // leaves are filled in from the target's key values; without one the key names are unknown, and the target on
      // the row is the only place the keys appear.
      final List<String> rawSegments = YangExplodeRule.parsePath(target.textValue());
      final ObjectNode root = factory.objectNode();
      ObjectNode parent = root;
      YangSchemaIndex.Node schemaNode = schema == null ? null : schema.getRoot();
      for (int i = 0; i < targetPath.size() - 1; i++) {
        final String segment = targetPath.get(i);
        schemaNode = schemaNode == null ? null : schemaNode.child(segment);
        final String raw = rawSegments.get(i);
        final int equals = raw.indexOf('=');
        if (equals >= 0) {
          parent = parent.putArray(segment).addObject();
          if (schemaNode != null) {
            putKeys(parent, schemaNode.getKeys(), raw.substring(equals + 1));
          }
        } else {
          parent = parent.putObject(segment);
        }
      }
      if (value != null && value.isObject()) {
        parent.setAll((ObjectNode) value);
        trees.add(new DataTree(root, extras, null));
      } else {
        trees.add(new DataTree(root, extras, targetPath));
      }
    }
  }

  /**
   * Whether the member {@code name} of the envelope level {@code levelName} is itself an envelope level. A data node
   * may also be called {@code message} (e.g. a BMP message structure), so a telemetry message is recognized by its
   * contents, and only its {@code payload} is unwrapped.
   */
  private static boolean isEnvelopeNode(@Nullable String levelName, String name, JsonNode value)
  {
    if (TELEMETRY_MESSAGE.equals(levelName)) {
      return TELEMETRY_PAYLOAD.equals(name);
    }
    if (TELEMETRY_MESSAGE.equals(name)) {
      return getByLocalName((ObjectNode) value, TELEMETRY_PAYLOAD) != null
             && getByLocalName((ObjectNode) value, TELEMETRY_METADATA) != null;
    }
    return ENVELOPE_NODES.contains(name);
  }

  /**
   * Copies the metadata of a telemetry message (everything but its payload) into {@code envelope}. Leaves are named
   * {@code <container>.<leaf>} after their immediate container ({@code telemetry-message-metadata.collection-timestamp},
   * {@code network-node-manifest.name}, {@code yang-push-subscription.id}); operator labels become
   * {@code labels.<label name>}; other lists (such as the subscription's modules) are kept as arrays.
   */
  private void copyTelemetryMetadata(ObjectNode message, ObjectNode envelope)
  {
    final Iterator<Map.Entry<String, JsonNode>> fields = message.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      final String local = localName(field.getKey());
      if (TELEMETRY_PAYLOAD.equals(local)) {
        continue;
      }
      if (field.getValue().isObject()) {
        flattenMetadata(local, (ObjectNode) field.getValue(), envelope);
      } else {
        envelope.set(field.getKey(), field.getValue());
      }
    }
  }

  private void flattenMetadata(String container, ObjectNode node, ObjectNode envelope)
  {
    final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      final String local = localName(field.getKey());
      final JsonNode value = field.getValue();
      if (value.isObject()) {
        flattenMetadata(local, (ObjectNode) value, envelope);
      } else if (value.isArray() && TELEMETRY_LABELS.equals(local)) {
        for (JsonNode label : value) {
          final JsonNode labelName = label.isObject() ? getByLocalName((ObjectNode) label, "name") : null;
          if (labelName != null && labelName.isTextual()) {
            envelope.set(TELEMETRY_LABELS + ANCESTOR_SEPARATOR + labelName.textValue(), labelValue((ObjectNode) label));
          }
        }
      } else {
        envelope.set(container + ANCESTOR_SEPARATOR + field.getKey(), value);
      }
    }
  }

  private static JsonNode labelValue(ObjectNode label)
  {
    for (String name : TELEMETRY_LABEL_VALUES) {
      final JsonNode value = getByLocalName(label, name);
      if (value != null) {
        return value;
      }
    }
    return JsonNodeFactory.instance.nullNode();
  }

  private void copyScalars(ObjectNode from, ObjectNode envelope, @Nullable String levelName)
  {
    final boolean isPushUpdate = PUSH_UPDATE.equals(levelName) || PUSH_CHANGE_UPDATE.equals(levelName);
    final Iterator<Map.Entry<String, JsonNode>> fields = from.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      if (field.getValue().isValueNode()) {
        final String local = localName(field.getKey());
        final String name;
        if (isPushUpdate && ID.equals(local)) {
          // YANG-Push "id" is the subscription id; give it a name that won't collide with data leaves called "id".
          name = SUBSCRIPTION_ID;
        } else if (EVENT_TIME_NAMES.contains(local)) {
          name = EVENT_TIME;
        } else {
          name = field.getKey();
        }
        envelope.set(name, field.getValue());
      }
    }
  }

  private void explode(
      ObjectNode node,
      List<String> path,
      int depth,
      ObjectNode base,
      ObjectNode ancestors,
      List<ObjectNode> rows
  )
  {
    final String segment = path.get(depth);
    final JsonNode child = getByLocalName(node, segment);
    if (child == null) {
      return;
    }
    final boolean last = depth == path.size() - 1;
    if (child.isArray()) {
      for (JsonNode entry : child) {
        if (!entry.isObject()) {
          continue;
        }
        if (last) {
          rows.add(buildRow(base, ancestors, (ObjectNode) entry));
        } else {
          explode((ObjectNode) entry, path, depth + 1, base, withScalars(ancestors, segment, (ObjectNode) entry), rows);
        }
      }
    } else if (child.isObject()) {
      if (last) {
        rows.add(buildRow(base, ancestors, (ObjectNode) child));
      } else {
        explode((ObjectNode) child, path, depth + 1, base, withScalars(ancestors, segment, (ObjectNode) child), rows);
      }
    }
  }

  /**
   * Returns {@code ancestors} plus the scalar leaves of {@code node} as {@code <segment>.<leaf>}, so rows exploded
   * below a list entry or container keep its keys and identifying leaves.
   */
  private static ObjectNode withScalars(ObjectNode ancestors, String segment, ObjectNode node)
  {
    ObjectNode result = ancestors;
    final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      if (field.getValue().isValueNode()) {
        if (result == ancestors) {
          result = ancestors.deepCopy();
        }
        result.set(localName(segment) + ANCESTOR_SEPARATOR + field.getKey(), field.getValue());
      }
    }
    return result;
  }

  /**
   * The row is the envelope, then ancestor leaves, then the entry. Data wins on a name clash, but the envelope's
   * value is kept as {@code envelope.<name>} so the publisher's identity (hostname, sequence number, ...) isn't lost,
   * e.g. when an incident embeds another device's notification.
   */
  private static ObjectNode buildRow(ObjectNode base, ObjectNode ancestors, ObjectNode entry)
  {
    final ObjectNode row = base.deepCopy();
    final Iterator<Map.Entry<String, JsonNode>> fields = base.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      final JsonNode data = entry.has(field.getKey()) ? entry.get(field.getKey()) : ancestors.get(field.getKey());
      if (data != null && !data.equals(field.getValue())) {
        row.set(ENVELOPE_PREFIX + field.getKey(), field.getValue());
      }
    }
    row.setAll(ancestors);
    row.setAll(entry);
    return row;
  }

  /**
   * Whether a value-less edit's target is the rule's node, one of its ancestors, or inside it.
   */
  private static boolean related(List<String> targetPath, YangExplodeRule rule)
  {
    final List<String> rulePath = rule.getSegments();
    final int common = Math.min(targetPath.size(), rulePath.size());
    for (int i = 0; i < common; i++) {
      if (!localName(targetPath.get(i)).equals(localName(rulePath.get(i)))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Node local names along a RESTCONF instance identifier, with list keys ({@code =...}) dropped.
   */
  static List<String> targetPath(String target)
  {
    final List<String> path = new ArrayList<>();
    for (String segment : YangExplodeRule.parsePath(target)) {
      final int equals = segment.indexOf('=');
      path.add(localName(equals < 0 ? segment : segment.substring(0, equals)));
    }
    return path;
  }

  /**
   * RFC 8040 section 3.5.3: key values in a list instance identifier are comma-separated, each percent-encoded.
   */
  private static void putKeys(ObjectNode entry, List<String> keys, String encodedValues)
  {
    final String[] values = encodedValues.split(",", -1);
    for (int i = 0; i < keys.size() && i < values.length; i++) {
      entry.put(keys.get(i), URLDecoder.decode(values[i], StandardCharsets.UTF_8));
    }
  }

  /**
   * Looks up a member by exact name first, then by local name, so paths work with or without module prefixes.
   */
  @Nullable
  private static JsonNode getByLocalName(ObjectNode node, String name)
  {
    final JsonNode exact = node.get(name);
    if (exact != null) {
      return exact;
    }
    final String local = localName(name);
    final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      if (local.equals(localName(field.getKey()))) {
        return field.getValue();
      }
    }
    return null;
  }

  static String localName(String name)
  {
    final int colon = name.indexOf(':');
    return colon < 0 ? name : name.substring(colon + 1);
  }

  private static class DataTree
  {
    final ObjectNode data;
    final ObjectNode extras;
    /**
     * Set for YANG-Patch edits without a value (delete, remove).
     */
    @Nullable
    final List<String> valuelessTarget;

    DataTree(ObjectNode data, ObjectNode extras, @Nullable List<String> valuelessTarget)
    {
      this.data = data;
      this.extras = extras;
      this.valuelessTarget = valuelessTarget;
    }
  }
}
