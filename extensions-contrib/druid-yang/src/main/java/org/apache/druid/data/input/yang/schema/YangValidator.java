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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates RFC 7951 JSON instance data against a {@link YangSchemaIndex}: every member must be a known node of the
 * right kind, values must match their types, list entries must have their keys, and unconditionally mandatory leaves
 * of structures and notifications must be present.
 *
 * <p>A document's top-level members are data nodes, notifications or {@code sx:structure} definitions, such as
 * {@code ietf-telemetry-message:message}. The contents of an {@code anydata} node, such as a telemetry message's
 * payload or a push-update's datastore contents, are validated the same way, from the top of the schema, as described
 * in draft-aelhassany-using-yang-library-to-validate-anydata. The exception is the {@code value} of a YANG-Patch
 * edit, which is relative to the edit's target and is not validated.
 *
 * <p>Mandatory leaves are not enforced in datastore data: subscription filters (RFC 8641 section 3.6) select parts of
 * the datastore, so a push-update's contents legitimately lack mandatory siblings of what was selected. List keys are
 * still required, since filters always include them.
 *
 * <p>{@code must} and {@code when} expressions, leafref instances, uniqueness and min/max-elements are not checked.
 * RFC 7952 metadata members (starting with {@code @}) are ignored.
 */
public class YangValidator
{
  private static final String YANG_PATCH_MODULE = "ietf-yang-patch";
  private static final String YANG_PATCH_VALUE = "value";

  private final YangSchemaIndex schema;
  private final int maxProblems;
  private final List<String> problems = new ArrayList<>();
  private int count;

  private YangValidator(YangSchemaIndex schema, int maxProblems)
  {
    this.schema = schema;
    this.maxProblems = maxProblems;
  }

  /**
   * @return the problems found, at most {@code maxProblems} of them; empty if the document is valid
   */
  public static Result validate(JsonNode document, YangSchemaIndex schema, int maxProblems)
  {
    final YangValidator validator = new YangValidator(schema, maxProblems);
    if (document.isObject()) {
      validator.topLevel(document, "");
    } else {
      validator.problem("", "expected a JSON object");
    }
    return new Result(validator.problems, validator.count);
  }

  private void topLevel(JsonNode object, String path)
  {
    final Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      if (field.getKey().startsWith("@")) {
        continue;
      }
      final String fieldPath = path + "/" + field.getKey();
      final YangSchemaIndex.Node node = schema.topLevel(field.getKey());
      if (node == null) {
        unknown(fieldPath, field.getKey(), null);
      } else {
        final boolean datastoreData =
            schema.getRoot().child(field.getKey()) == node && !schema.isTopLevelNotification(node);
        node(node, field.getValue(), fieldPath, !datastoreData);
      }
    }
  }

  /**
   * @param checkMandatory whether to report missing mandatory leaves below {@code node}
   */
  private void node(YangSchemaIndex.Node node, JsonNode value, String path, boolean checkMandatory)
  {
    if (node.getModule() != null && schema.getBrokenModules().containsKey(node.getModule())) {
      brokenModule(path, node.getModule());
      return;
    }
    switch (node.getKind()) {
      case CONTAINER:
        if (value.isObject()) {
          children(node, value, path, checkMandatory);
        } else {
          problem(path, "expected a container (JSON object)");
        }
        break;
      case LIST:
        if (!value.isArray()) {
          problem(path, "expected a list (JSON array)");
          break;
        }
        for (int i = 0; i < value.size(); i++) {
          final JsonNode entry = value.get(i);
          final String entryPath = path + "[" + i + "]";
          if (!entry.isObject()) {
            problem(entryPath, "expected a list entry (JSON object)");
            continue;
          }
          for (String key : node.getKeys()) {
            if (member(entry, node, key) == null) {
              problem(entryPath, StringUtils.format("missing list key [%s]", key));
            }
          }
          children(node, entry, entryPath, checkMandatory);
        }
        break;
      case LEAF:
        leaf(node, value, path);
        break;
      case LEAF_LIST:
        if (!value.isArray()) {
          problem(path, "expected a leaf-list (JSON array)");
          break;
        }
        for (int i = 0; i < value.size(); i++) {
          leaf(node, value.get(i), path + "[" + i + "]");
        }
        break;
      case ANYDATA:
        if (YANG_PATCH_MODULE.equals(node.getModule()) && YANG_PATCH_VALUE.equals(node.getName())) {
          break;
        }
        if (value.isObject()) {
          topLevel(value, path);
        } else {
          problem(path, "expected anydata (JSON object)");
        }
        break;
      default:
        problem(path, "unexpected node");
    }
  }

  private void children(YangSchemaIndex.Node node, JsonNode object, String path, boolean checkMandatory)
  {
    final Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
    while (fields.hasNext()) {
      final Map.Entry<String, JsonNode> field = fields.next();
      if (field.getKey().startsWith("@")) {
        continue;
      }
      final String fieldPath = path + "/" + field.getKey();
      final YangSchemaIndex.Node child = node.child(field.getKey());
      if (child == null) {
        unknown(fieldPath, field.getKey(), node);
      } else {
        node(child, field.getValue(), fieldPath, checkMandatory);
      }
    }
    if (!checkMandatory) {
      return;
    }
    // Children are indexed by both local and qualified name; visit each once.
    final Set<YangSchemaIndex.Node> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (YangSchemaIndex.Node child : node.getChildren().values()) {
      if (child.isMandatory() && seen.add(child) && member(object, node, child.getName()) == null) {
        problem(path, StringUtils.format("missing mandatory leaf [%s]", child.getName()));
      }
    }
  }

  private void leaf(YangSchemaIndex.Node node, JsonNode value, String path)
  {
    if (value.isObject() || (value.isArray() && node.getValueType() != YangSchemaIndex.ValueType.EMPTY)) {
      problem(path, "expected a leaf value");
      return;
    }
    final YangSchemaIndex.ValueCheck check = node.getValueCheck();
    final String problem = check == null ? null : check.check(value);
    if (problem != null) {
      problem(path, problem);
    }
  }

  /**
   * The member of {@code object} for the child {@code name} of {@code node}, under its local or qualified name.
   */
  @Nullable
  private static JsonNode member(JsonNode object, YangSchemaIndex.Node node, String name)
  {
    final JsonNode local = object.get(name);
    if (local != null) {
      return local;
    }
    final YangSchemaIndex.Node child = node.child(name);
    return child == null || child.getModule() == null ? null : object.get(child.getModule() + ":" + name);
  }

  private void unknown(String path, String name, @Nullable YangSchemaIndex.Node parent)
  {
    final int colon = name.indexOf(':');
    final String module = colon < 0 ? null : name.substring(0, colon);
    if (module != null && schema.getBrokenModules().containsKey(module)) {
      brokenModule(path, module);
    } else {
      problem(path, parent == null ? "unknown top-level node" : "unknown node");
    }
  }

  private void brokenModule(String path, String module)
  {
    problem(
        path,
        StringUtils.format(
            "defined by module [%s], which failed to compile: %s",
            module,
            schema.getBrokenModules().get(module)
        )
    );
  }

  private void problem(String path, String message)
  {
    count++;
    if (problems.size() < maxProblems) {
      problems.add(StringUtils.format("%s: %s", path.isEmpty() ? "/" : path, message));
    }
  }

  public static class Result
  {
    private final List<String> problems;
    private final int count;

    Result(List<String> problems, int count)
    {
      this.problems = problems;
      this.count = count;
    }

    public boolean isValid()
    {
      return count == 0;
    }

    /**
     * The first problems found, each as {@code /path: message}.
     */
    public List<String> getProblems()
    {
      return problems;
    }

    /**
     * The total number of problems, which may exceed {@code getProblems().size()}.
     */
    public int getCount()
    {
      return count;
    }
  }
}
