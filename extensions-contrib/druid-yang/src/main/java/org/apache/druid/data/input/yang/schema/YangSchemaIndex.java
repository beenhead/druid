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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The parts of a compiled YANG schema that ingestion needs: the data tree (containers, lists, leaves and leaf-lists,
 * with choices, cases and uses already flattened and augments applied), list keys, and a coarse value type per leaf.
 * Built by {@link YangSchemaCompiler}; free of any parser types.
 */
public class YangSchemaIndex
{
  public enum Kind
  {
    ROOT,
    CONTAINER,
    LIST,
    LEAF,
    LEAF_LIST
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
    private final Map<String, Node> children = new HashMap<>();

    public Node(String name, @Nullable String module, Kind kind, @Nullable ValueType valueType, List<String> keys)
    {
      this.name = name;
      this.module = module;
      this.kind = kind;
      this.valueType = valueType;
      this.keys = ImmutableList.copyOf(keys);
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
  private final List<String> modules;

  public YangSchemaIndex(Node root, List<String> modules)
  {
    this.root = root;
    this.modules = ImmutableList.copyOf(modules);
  }

  public Node getRoot()
  {
    return root;
  }

  /**
   * Names of the modules the schema was compiled from.
   */
  public List<String> getModules()
  {
    return modules;
  }
}
