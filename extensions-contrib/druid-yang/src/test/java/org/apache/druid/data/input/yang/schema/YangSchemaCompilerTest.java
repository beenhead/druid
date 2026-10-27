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
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class YangSchemaCompilerTest
{
  static Path modulesDir()
  {
    try {
      return Paths.get(YangSchemaCompilerTest.class.getResource("/yang/modules").toURI());
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  static Map<String, String> modules()
  {
    return DirectoryYangSchemaSource.readModules(modulesDir());
  }

  private static YangSchemaIndex.Node path(YangSchemaIndex index, String... names)
  {
    YangSchemaIndex.Node node = index.getRoot();
    for (String name : names) {
      node = node.child(name);
      Assertions.assertNotNull(node, "missing " + name);
    }
    return node;
  }

  @Test
  public void testDataTreeTypesAndKeys()
  {
    final YangSchemaIndex index = YangSchemaCompiler.compile(modules());

    Assertions.assertTrue(index.getModules().contains("ietf-interfaces"));
    Assertions.assertTrue(index.getModules().contains("acme-if-ext"));

    final YangSchemaIndex.Node iface = path(index, "interfaces", "interface");
    Assertions.assertEquals(YangSchemaIndex.Kind.LIST, iface.getKind());
    Assertions.assertEquals(ImmutableList.of("name"), iface.getKeys());

    // typedef chains resolve to built-in types: yang:counter64 -> uint64, yang:gauge64 -> uint64
    Assertions.assertEquals(
        YangSchemaIndex.ValueType.INTEGER,
        path(index, "interfaces", "interface", "statistics", "in-octets").getValueType()
    );
    Assertions.assertEquals(YangSchemaIndex.ValueType.INTEGER, path(index, "interfaces", "interface", "speed").getValueType());
    Assertions.assertEquals(
        YangSchemaIndex.ValueType.OTHER,
        path(index, "interfaces", "interface", "statistics", "discontinuity-time").getValueType()
    );
    Assertions.assertEquals(YangSchemaIndex.ValueType.BOOLEAN, path(index, "interfaces", "interface", "enabled").getValueType());
    Assertions.assertEquals(
        YangSchemaIndex.Kind.LEAF_LIST,
        path(index, "interfaces", "interface", "higher-layer-if").getKind()
    );

    // augments from ietf-ip and acme-if-ext are merged into interface
    Assertions.assertEquals(
        YangSchemaIndex.ValueType.EMPTY,
        path(index, "interfaces", "interface", "ipv6", "neighbor", "is-router").getValueType()
    );
    Assertions.assertEquals(ImmutableList.of("ip"), path(index, "interfaces", "interface", "ipv6", "address").getKeys());
    Assertions.assertEquals(
        YangSchemaIndex.ValueType.OTHER,
        path(index, "interfaces", "interface", "optics", "serial-number").getValueType()
    );
    Assertions.assertEquals(
        YangSchemaIndex.ValueType.DECIMAL,
        path(index, "interfaces", "interface", "optics", "temperature").getValueType()
    );
    Assertions.assertEquals(ImmutableList.of("index"), path(index, "interfaces", "interface", "optics", "lane").getKeys());

    // qualified lookup
    Assertions.assertSame(
        path(index, "interfaces", "interface", "optics"),
        path(index, "interfaces", "interface").child("acme-if-ext:optics")
    );
    Assertions.assertEquals("ietf-ip", path(index, "interfaces", "interface", "ipv6").getModule());
  }

  @Test
  public void testCompilationsAreIndependent()
  {
    final YangSchemaIndex all = YangSchemaCompiler.compile(modules());
    final Map<String, String> modules = modules();
    final YangSchemaIndex withoutAcme = YangSchemaCompiler.compile(
        ImmutableMap.of(
            "ietf-interfaces", modules.get("ietf-interfaces@2018-02-20.yang"),
            "ietf-yang-types", modules.get("ietf-yang-types@2013-07-15.yang")
        )
    );

    Assertions.assertNotNull(path(all, "interfaces", "interface").child("optics"));
    Assertions.assertNull(path(withoutAcme, "interfaces", "interface").child("optics"));
    Assertions.assertEquals(2, withoutAcme.getModules().size());
  }

  @Test
  public void testMissingImportIsInvalid()
  {
    final Map<String, String> modules = modules();
    Assertions.assertThrows(
        YangSchemaCompiler.YangSchemaException.class,
        () -> YangSchemaCompiler.compile(ImmutableMap.of("acme", modules.get("acme-if-ext@2026-09-30.yang")))
    );
  }

  @Test
  public void testUnparseableModule()
  {
    Assertions.assertThrows(
        YangSchemaCompiler.YangSchemaException.class,
        () -> YangSchemaCompiler.compile(ImmutableMap.of("broken", "module broken { this is not yang"))
    );
  }
}
