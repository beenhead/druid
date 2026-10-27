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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.yang.schema.DirectoryYangSchemaSource;
import org.apache.druid.data.input.yang.schema.YangSchemaIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

public class YangSchemaTypingTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final YangExplodeRule INTERFACES = new YangExplodeRule("interfaces", "interfaces/interface");
  private static YangSchemaIndex schema;

  @BeforeAll
  public static void setUp() throws URISyntaxException
  {
    schema = new DirectoryYangSchemaSource(
        Paths.get(YangSchemaTypingTest.class.getResource("/yang/modules").toURI()).toString()
    ).getSchema(null);
  }

  private static YangJsonTransformer transformer(boolean coerceTypes)
  {
    return new YangJsonTransformer(ImmutableList.of(INTERFACES), true, coerceTypes, true, false);
  }

  @Test
  public void testSchemaTypesKnownLeaves() throws IOException
  {
    final List<ObjectNode> rows = transformer(true).transform(
        YangJsonTransformerTest.resource("schema-typed-interfaces.json"),
        schema
    );

    Assertions.assertEquals(1, rows.size());
    final ObjectNode row = rows.get(0);

    // envelope, including the draft's notification-contents wrapper
    Assertions.assertEquals("2026-09-30T12:00:00.000Z", row.get(YangJsonTransformer.EVENT_TIME).textValue());
    Assertions.assertEquals(1011L, row.get("subscription-id").longValue());

    // counter64 / gauge64 are uint64: strings become longs, uint64 max a double
    Assertions.assertEquals(100_000_000_000L, row.get("speed").longValue());
    Assertions.assertEquals(123456789012L, row.get("statistics").get("in-octets").longValue());
    Assertions.assertTrue(row.get("statistics").get("out-octets").isDouble());
    Assertions.assertEquals(5, row.get("statistics").get("in-errors").intValue());
    Assertions.assertTrue(row.get("statistics").get("discontinuity-time").isTextual());

    // strings that look like numbers stay strings when the schema says string
    Assertions.assertEquals("42", row.get("description").textValue());
    Assertions.assertEquals("12345", row.get("optics").get("serial-number").textValue());

    // decimal64
    Assertions.assertEquals(41.5, row.get("optics").get("temperature").doubleValue(), 0);
    Assertions.assertEquals(-3.21, row.get("optics").get("rx-power").doubleValue(), 0);
    Assertions.assertEquals(-3.20, row.get("optics").get("lane").get(0).get("rx-power").doubleValue(), 0);

    // empty
    Assertions.assertTrue(row.get("ipv6").get("neighbor").get(0).get("is-router").booleanValue());

    // leaf-list of leafrefs stays as is
    Assertions.assertEquals("Port-channel1", row.get("higher-layer-if").get(0).textValue());

    // nodes the schema doesn't know fall back to the heuristic
    Assertions.assertTrue(row.get("maintenance-mode").booleanValue());
    Assertions.assertEquals("007", row.get("ticket").textValue());
    Assertions.assertEquals(12.5, row.get("utilisation").doubleValue(), 0);
  }

  @Test
  public void testWithoutHeuristicUnknownNodesStayAsEncoded() throws IOException
  {
    final ObjectNode row = transformer(false).transform(
        YangJsonTransformerTest.resource("schema-typed-interfaces.json"),
        schema
    ).get(0);

    Assertions.assertEquals(123456789012L, row.get("statistics").get("in-octets").longValue());
    Assertions.assertTrue(row.get("maintenance-mode").isArray());
    Assertions.assertEquals("12.5", row.get("utilisation").textValue());
  }

  @Test
  public void testHeuristicWithoutSchemaGetsStringsWrong() throws IOException
  {
    // The motivating case: without a schema, a numeric-looking string leaf is turned into a number.
    final ObjectNode row = transformer(true).transform(
        YangJsonTransformerTest.resource("schema-typed-interfaces.json")
    ).get(0);

    Assertions.assertEquals(42L, row.get("description").longValue());
    Assertions.assertEquals(12345L, row.get("optics").get("serial-number").longValue());
  }

  @Test
  public void testChangeUpdateKeysRestoredFromTarget() throws IOException
  {
    final List<ObjectNode> rows = transformer(true).transform(
        YangJsonTransformerTest.resource("restconf-change-update.json"),
        schema
    );

    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals("GigabitEthernet0/0/7", rows.get(0).get("name").textValue());
    // the delete's target is .../interface=GigabitEthernet0%2F0%2F9/ietf-ip:ipv6; the key comes from the target
    Assertions.assertEquals("delete", rows.get(1).get(YangJsonTransformer.YANG_OPERATION).textValue());
    Assertions.assertEquals("GigabitEthernet0/0/9", rows.get(1).get("name").textValue());
  }

  @Test
  public void testChangeUpdateInsideNestedListTypesKeys() throws IOException
  {
    final List<ObjectNode> rows = new YangJsonTransformer(
        ImmutableList.of(new YangExplodeRule("lanes", "interfaces/interface/optics/lane")),
        true,
        true,
        true,
        false
    ).transform(
        MAPPER.readTree(
            "{\"ietf-yang-push:push-change-update\": {\"id\": 1, \"datastore-changes\": {\"yang-patch\": {"
            + "\"patch-id\": \"p\", \"edit\": [{\"edit-id\": \"e\", \"operation\": \"merge\","
            + " \"target\": \"/ietf-interfaces:interfaces/interface=Hu0%2F1%2F0/acme-if-ext:optics/lane=2\","
            + " \"value\": {\"acme-if-ext:lane\": [{\"index\": 2, \"rx-power\": \"-4.10\"}]}}]}}}}"
        ),
        schema
    );

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("Hu0/1/0", rows.get(0).get("interface.name").textValue());
    Assertions.assertEquals(2, rows.get(0).get("index").intValue());
    Assertions.assertEquals(-4.10, rows.get(0).get("rx-power").doubleValue(), 0);
  }

  @Test
  public void testChangeUpdateToContainerInsideListEntry() throws IOException
  {
    final List<ObjectNode> rows = transformer(true).transform(
        MAPPER.readTree(
            "{\"ietf-yang-push:push-change-update\": {\"id\": 1, \"datastore-changes\": {\"yang-patch\": {"
            + "\"patch-id\": \"p\", \"edit\": [{\"edit-id\": \"e\", \"operation\": \"replace\","
            + " \"target\": \"/ietf-interfaces:interfaces/interface=eth0/statistics\","
            + " \"value\": {\"ietf-interfaces:statistics\": {\"in-octets\": \"5\"}}}]}}}}"
        ),
        schema
    );

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("eth0", rows.get(0).get("name").textValue());
    Assertions.assertEquals(5L, rows.get(0).get("statistics").get("in-octets").longValue());
  }
}
