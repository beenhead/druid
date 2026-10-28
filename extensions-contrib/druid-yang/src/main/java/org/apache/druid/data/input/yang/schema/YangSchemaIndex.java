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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The parts of a compiled YANG schema that ingestion needs: the data tree (containers, lists, leaves, leaf-lists and
 * anydata, with choices, cases and uses already flattened and augments applied), list keys, a coarse value type per
 * leaf, and what {@link YangValidator} checks. Built by {@link YangSchemaCompiler}; free of any parser types.
 */
public class YangSchemaIndex
{
  public enum Kind
  {
    ROOT,
    CONTAINER,
    LIST,
    LEAF,
    LEAF_LIST,
    /**
     * Instance data of any schema, e.g. the payload of a telemetry message or the datastore contents of a push-update.
     */
    ANYDATA
  }

  /**
   * Checks a leaf or leaf-list value against its YANG type.
   */
  public interface ValueCheck
  {
    /**
     * @return why the value is invalid, or null if it is valid
     */
    @Nullable
    String check(JsonNode value);
  }

  /**
   * How a leaf's value should be represented, derived from its built-in type.
   */
  public enum ValueType
  {
    /**
     * Any integer type. RFC 7951 encodes 64-bit ones as strings; list keys taken from instance identifiers are always
     * strings. Values beyond the long range become doubles (appropriate for counters and gauges).
     */
    INTEGER,
    /**
     * A uint64 that is not a counter or gauge, typically an identifier or serial number. Values beyond the long range
     * stay strings so they remain exact.
     */
    INTEGER_EXACT,
    /**
     * decimal64, encoded as a string per RFC 7951.
     */
    DECIMAL,
    BOOLEAN,
    /**
     * The "empty" type, encoded as [null].
     */
    EMPTY,
    /**
     * Everything that stays as it is: strings, enumerations, identityrefs, bits, binary, instance-identifiers, and
     * unions whose first member is not numeric.
     */
    OTHER
  }

  public static class Node
  {
    private final String name;
    @Nullable
    private final String module;
    private final Kind kind;
    @Nullable
    private final ValueType valueType;
    private final List<String> keys;
    private final boolean mandatory;
    @Nullable
    private final ValueCheck valueCheck;
    private final Map<String, Node> children = new HashMap<>();

    public Node(String name, @Nullable String module, Kind kind, @Nullable ValueType valueType, List<String> keys)
    {
      this(name, module, kind, valueType, keys, false, null);
    }

    public Node(
        String name,
        @Nullable String module,
        Kind kind,
        @Nullable ValueType valueType,
        List<String> keys,
        boolean mandatory,
        @Nullable ValueCheck valueCheck
    )
    {
      this.name = name;
      this.module = module;
      this.kind = kind;
      this.valueType = valueType;
      this.keys = ImmutableList.copyOf(keys);
      this.mandatory = mandatory;
      this.valueCheck = valueCheck;
    }

    public String getName()
    {
      return name;
    }

    @Nullable
    public String getModule()
    {
      return module;
    }

    public Kind getKind()
    {
      return kind;
    }

    /**
     * Set for leaves and leaf-lists.
     */
    @Nullable
    public ValueType getValueType()
    {
      return valueType;
    }

    /**
     * Key leaf names, in order, for lists; empty otherwise.
     */
    public List<String> getKeys()
    {
      return keys;
    }

    /**
     * Whether the node must be present whenever its parent is. Only set where that holds unconditionally: not for
     * nodes in a choice, under a {@code when} or {@code if-feature}, or added by an augment.
     */
    public boolean isMandatory()
    {
      return mandatory;
    }

    /**
     * Set for leaves and leaf-lists.
     */
    @Nullable
    public ValueCheck getValueCheck()
    {
      return valueCheck;
    }

    /**
     * Looks up a child by local name or by qualified {@code module:name}. A qualified name is needed only when two
     * modules put nodes with the same local name at the same level.
     */
    @Nullable
    public Node child(String name)
    {
      return children.get(name);
    }

    public Map<String, Node> getChildren()
    {
      return children;
    }

    void addChild(Node child)
    {
      if (child.module != null) {
        children.put(child.module + ":" + child.name, child);
      }
      children.putIfAbsent(child.name, child);
    }
  }

  private final Node root;
  private final Node structures;
  private final List<String> modules;
  private final Set<String> notifications;
  private final Map<String, String> brokenModules;

  public YangSchemaIndex(Node root, List<String> modules)
  {
    this(
        root,
        new Node("", null, Kind.ROOT, null, ImmutableList.of()),
        ImmutableSet.of(),
        modules,
        ImmutableMap.of()
    );
  }

  public YangSchemaIndex(
      Node root,
      Node structures,
      Set<String> notifications,
      List<String> modules,
      Map<String, String> brokenModules
  )
  {
    this.root = root;
    this.structures = structures;
    this.notifications = ImmutableSet.copyOf(notifications);
    this.modules = ImmutableList.copyOf(modules);
    this.brokenModules = ImmutableMap.copyOf(brokenModules);
  }

  public Node getRoot()
  {
    return root;
  }

  /**
   * Top-level {@code sx:structure} definitions (RFC 8791), such as {@code ietf-yp-notification:envelope} and
   * {@code ietf-telemetry-message:message}. They are not part of the data tree, but messages are instances of them.
   */
  public Node getStructures()
  {
    return structures;
  }

  /**
   * Whether {@code node} is a top-level notification rather than a datastore data node.
   */
  public boolean isTopLevelNotification(Node node)
  {
    return notifications.contains(node.getModule() + ":" + node.getName());
  }

  /**
   * Looks up a top-level data node, notification or structure.
   */
  @Nullable
  public Node topLevel(String name)
  {
    final Node node = root.child(name);
    return node != null ? node : structures.child(name);
  }

  /**
   * Names of the modules the schema was compiled from.
   */
  public List<String> getModules()
  {
    return modules;
  }

  /**
   * Modules that failed to parse or validate, with the first error for each. The rest of the schema is still usable;
   * nodes from these modules may be missing or incomplete.
   */
  public Map<String, String> getBrokenModules()
  {
    return brokenModules;
  }
}
