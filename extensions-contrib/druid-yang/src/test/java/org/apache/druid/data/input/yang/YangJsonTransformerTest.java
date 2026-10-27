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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class YangJsonTransformerTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final YangExplodeRule INTERFACES = new YangExplodeRule("interfaces", "interfaces/interface");
  private static final YangExplodeRule BGP_NEIGHBORS = new YangExplodeRule(
      "bgp-neighbors",
      "network-instances/network-instance/protocols/protocol/bgp/neighbors/neighbor"
  );

  static JsonNode resource(String name) throws IOException
  {
    try (InputStream in = YangJsonTransformerTest.class.getResourceAsStream("/yang/" + name)) {
      return MAPPER.readTree(in);
    }
  }

  private static YangJsonTransformer transformer(YangExplodeRule... rules)
  {
    return new YangJsonTransformer(ImmutableList.copyOf(rules), true, true, true, false);
  }

  private static YangJsonTransformer path(String path)
  {
    return transformer(new YangExplodeRule(null, path));
  }

  @Test
  public void testExplodeInterfaces() throws IOException
  {
    final List<ObjectNode> rows = transformer(INTERFACES).transform(resource("push-update-interfaces.json"));

    Assertions.assertEquals(2, rows.size());

    final ObjectNode eth0 = rows.get(0);
    Assertions.assertEquals("2026-09-30T12:00:00.000Z", eth0.get(YangJsonTransformer.EVENT_TIME).textValue());
    Assertions.assertEquals(1011L, eth0.get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    Assertions.assertEquals("interfaces", eth0.get(YangJsonTransformer.YANG_RULE).textValue());
    Assertions.assertEquals("push-update", eth0.get(YangJsonTransformer.YANG_UPDATE).textValue());
    Assertions.assertEquals("eth0", eth0.get("name").textValue());
    // identityref values keep their prefix; only member names are stripped
    Assertions.assertEquals("iana-if-type:ethernetCsmacd", eth0.get("type").textValue());
    Assertions.assertTrue(eth0.get("maintenance-mode").booleanValue());
    Assertions.assertTrue(eth0.get("statistics").get("in-octets").isLong());
    Assertions.assertEquals(123456789012L, eth0.get("statistics").get("in-octets").longValue());
    // date-and-time strings are not numbers
    Assertions.assertTrue(eth0.get("statistics").get("discontinuity-time").isTextual());
    Assertions.assertTrue(eth0.has("ipv4"));
    Assertions.assertFalse(eth0.has("ietf-ip:ipv4"));

    final ObjectNode eth1 = rows.get(1);
    Assertions.assertEquals("eth1", eth1.get("name").textValue());
    // uint64 max does not fit in a long
    Assertions.assertTrue(eth1.get("statistics").get("in-octets").isDouble());
    Assertions.assertEquals(1.8446744073709552E19, eth1.get("statistics").get("in-octets").doubleValue(), 0);
  }

  @Test
  public void testExplodeNestedListCopiesAncestorKeys() throws IOException
  {
    final List<ObjectNode> rows = path("ietf-interfaces:interfaces/interface/ietf-ip:ipv4/address")
        .transform(resource("push-update-interfaces.json"));

    // eth1 has no ipv4 container, so only eth0's two addresses produce rows
    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals("192.0.2.1", rows.get(0).get("ip").textValue());
    Assertions.assertEquals("192.0.2.2", rows.get(1).get("ip").textValue());
    for (ObjectNode row : rows) {
      Assertions.assertEquals("eth0", row.get("interface.name").textValue());
      Assertions.assertEquals("up", row.get("interface.oper-status").textValue());
      Assertions.assertEquals(24L, row.get("prefix-length").longValue());
      Assertions.assertEquals(1011L, row.get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
      // the rule is named after its path when no name is given
      Assertions.assertEquals(
          "ietf-interfaces:interfaces/interface/ietf-ip:ipv4/address",
          row.get(YangJsonTransformer.YANG_RULE).textValue()
      );
      // scalar leaves of enclosing containers are copied too; nested containers are not
      Assertions.assertEquals(1500L, row.get("ipv4.mtu").longValue());
      Assertions.assertFalse(row.has("interface.statistics"));
    }
  }

  @Test
  public void testNotificationEnvelopeNormalizesEventTime() throws IOException
  {
    final List<ObjectNode> rows = transformer(INTERFACES).transform(resource("envelope-interfaces.json"));

    Assertions.assertEquals(1, rows.size());
    final ObjectNode row = rows.get(0);
    Assertions.assertEquals("2026-09-30T12:00:05.000Z", row.get(YangJsonTransformer.EVENT_TIME).textValue());
    Assertions.assertFalse(row.has("event-time"));
    Assertions.assertEquals("router-1.example.net", row.get("hostname").textValue());
    Assertions.assertEquals(42L, row.get("sequence-number").longValue());
    Assertions.assertEquals(7L, row.get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    Assertions.assertEquals("ge-0/0/0", row.get("name").textValue());
    Assertions.assertEquals(2000L, row.get("statistics").get("out-octets").longValue());
  }

  @Test
  public void testTelemetryMessageWithBarePushUpdate() throws IOException
  {
    // Figure 5 of draft-ietf-nmop-message-broker-telemetry-message (NetGauze)
    final List<ObjectNode> rows = transformer(INTERFACES).transform(resource("telemetry-message-netgauze.json"));

    Assertions.assertEquals(1, rows.size());
    final ObjectNode row = rows.get(0);
    Assertions.assertEquals("eth0", row.get("name").textValue());
    Assertions.assertEquals("down", row.get("oper-status").textValue());
    Assertions.assertEquals("push-update", row.get(YangJsonTransformer.YANG_UPDATE).textValue());
    Assertions.assertEquals(89L, row.get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    // no notification time in the payload: the node's export time stands in
    Assertions.assertEquals("2024-02-14T12:10:10.10+01:00", row.get(YangJsonTransformer.EVENT_TIME).textValue());

    Assertions.assertEquals("pe1", row.get("network-node-manifest.name").textValue());
    Assertions.assertEquals("collector-1", row.get("data-collection-manifest.name").textValue());
    Assertions.assertEquals(
        "2024-02-14T12:10:10.12+01:00",
        row.get("telemetry-message-metadata.collection-timestamp").textValue()
    );
    Assertions.assertEquals("192.168.1.100", row.get("telemetry-message-metadata.export-address").textValue());
    Assertions.assertEquals(9991L, row.get("telemetry-message-metadata.collection-port").longValue());
    Assertions.assertEquals(89L, row.get("yang-push-subscription.id").longValue());
    Assertions.assertEquals("/ietf-interfaces:interfaces", row.get("yang-push-subscription.xpath-filter").textValue());
    Assertions.assertTrue(row.get("yang-push-subscription.module").isArray());
    Assertions.assertEquals("name", row.get("labels.platform-name").textValue());
    Assertions.assertFalse(row.has("payload"));
    Assertions.assertFalse(row.has("network-node-manifest"));
  }

  @Test
  public void testTelemetryMessageWithNotificationEnvelope() throws IOException
  {
    // Figure 6 of draft-ietf-nmop-message-broker-telemetry-message (pmacct)
    final List<ObjectNode> rows = transformer(new YangExplodeRule("interfaces", "interfaces/interface"))
        .transform(resource("telemetry-message-pmacct.json"));

    Assertions.assertEquals(1, rows.size());
    final ObjectNode row = rows.get(0);
    Assertions.assertEquals("TenGigE0/0/0/23", row.get("name").textValue());
    Assertions.assertEquals(0L, row.get("state").get("counters").get("in-octets").longValue());
    // the notification's own time wins over the collector's timestamps
    Assertions.assertEquals("2025-07-18T09:51:00.002Z", row.get(YangJsonTransformer.EVENT_TIME).textValue());
    Assertions.assertEquals("ipf-zbl1327-r-daisy-91", row.get("hostname").textValue());
    Assertions.assertEquals(1L, row.get("sequence-number").longValue());
    Assertions.assertEquals(210L, row.get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    Assertions.assertEquals("pmacct", row.get("data-collection-manifest.vendor").textValue());
    Assertions.assertEquals(3000L, row.get("periodic.period").longValue());
    Assertions.assertEquals(
        "openconfig-interfaces:interfaces/interface/state/counters",
        row.get("yang-push-subscription.datastore-xpath-filter").textValue()
    );
    Assertions.assertEquals(3L, row.get("labels.seq").longValue());
    Assertions.assertEquals("json", row.get("labels.serialization").textValue());
  }

  @Test
  public void testDataNodeNamedMessageIsNotUnwrapped() throws IOException
  {
    final JsonNode document = MAPPER.readTree(
        "{\"acme-log:message\": {\"payload\": {\"text\": \"hello\"}, \"severity\": \"info\"}}"
    );
    final List<ObjectNode> rows = path("message").transform(document);

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("info", rows.get(0).get("severity").textValue());
    Assertions.assertEquals("hello", rows.get(0).get("payload").get("text").textValue());
  }

  @Test
  public void testMultipleRulesOnlyMatchingRuleEmits() throws IOException
  {
    final List<ObjectNode> rows = transformer(INTERFACES, BGP_NEIGHBORS).transform(resource("envelope-bgp.json"));

    // two neighbours in the default VRF, one in CUSTOMER-A
    Assertions.assertEquals(3, rows.size());
    for (ObjectNode row : rows) {
      Assertions.assertEquals("bgp-neighbors", row.get(YangJsonTransformer.YANG_RULE).textValue());
      Assertions.assertEquals("openconfig-policy-types:BGP", row.get("protocol.identifier").textValue());
      Assertions.assertEquals("BGP", row.get("protocol.name").textValue());
      Assertions.assertTrue(row.get("state").get("messages").get("received").get("UPDATE").isLong());
    }
    Assertions.assertEquals("default", rows.get(0).get("network-instance.name").textValue());
    Assertions.assertEquals("CUSTOMER-A", rows.get(2).get("network-instance.name").textValue());
    Assertions.assertTrue(rows.get(1).get("neighbor-address").textValue().contains(":"));
  }

  @Test
  public void testDocumentMatchingTwoRulesEmitsForBoth() throws IOException
  {
    final List<ObjectNode> rows = transformer(
        INTERFACES,
        new YangExplodeRule("addresses", "interfaces/interface/ipv4/address")
    ).transform(resource("push-update-interfaces.json"));

    Assertions.assertEquals(4, rows.size());
    Assertions.assertEquals(2, rows.stream().filter(r -> "interfaces".equals(r.get("yang-rule").textValue())).count());
    Assertions.assertEquals(2, rows.stream().filter(r -> "addresses".equals(r.get("yang-rule").textValue())).count());
  }

  @Test
  public void testChangeUpdateEditsBecomeRows() throws IOException
  {
    final List<ObjectNode> rows = transformer(INTERFACES).transform(resource("restconf-change-update.json"));

    Assertions.assertEquals(2, rows.size());

    final ObjectNode merge = rows.get(0);
    Assertions.assertEquals("push-change-update", merge.get(YangJsonTransformer.YANG_UPDATE).textValue());
    Assertions.assertEquals("merge", merge.get(YangJsonTransformer.YANG_OPERATION).textValue());
    Assertions.assertEquals(
        "/ietf-interfaces:interfaces/interface=GigabitEthernet0%2F0%2F7",
        merge.get(YangJsonTransformer.YANG_TARGET).textValue()
    );
    Assertions.assertEquals("interfaces", merge.get(YangJsonTransformer.YANG_RULE).textValue());
    Assertions.assertEquals("GigabitEthernet0/0/7", merge.get("name").textValue());
    Assertions.assertEquals("down", merge.get("oper-status").textValue());
    Assertions.assertEquals(3010L, merge.get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    Assertions.assertEquals("p-40", merge.get("patch-id").textValue());
    Assertions.assertEquals("2026-09-30T13:31:41.118Z", merge.get(YangJsonTransformer.EVENT_TIME).textValue());

    // a delete has no value, but its target is inside an interface entry
    final ObjectNode delete = rows.get(1);
    Assertions.assertEquals("delete", delete.get(YangJsonTransformer.YANG_OPERATION).textValue());
    Assertions.assertEquals("interfaces", delete.get(YangJsonTransformer.YANG_RULE).textValue());
    Assertions.assertTrue(delete.get(YangJsonTransformer.YANG_TARGET).textValue().endsWith("/ietf-ip:ipv6"));
    Assertions.assertFalse(delete.has("name"));
  }

  @Test
  public void testChangeUpdateToContainerInsideListEntry() throws IOException
  {
    final List<ObjectNode> rows = transformer(INTERFACES).transform(MAPPER.readTree(
        "{\"ietf-yang-push:push-change-update\": {\"id\": 1, \"datastore-changes\": {\"yang-patch\": {"
        + "\"patch-id\": \"p\", \"edit\": [{\"edit-id\": \"e\", \"operation\": \"replace\","
        + " \"target\": \"/ietf-interfaces:interfaces/interface=eth0/statistics\","
        + " \"value\": {\"ietf-interfaces:statistics\": {\"in-octets\": \"5\"}}}]}}}}"
    ));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(5L, rows.get(0).get("statistics").get("in-octets").longValue());
    // the list key is only in the target
    Assertions.assertFalse(rows.get(0).has("name"));
  }

  @Test
  public void testUnmatched() throws IOException
  {
    Assertions.assertTrue(transformer(BGP_NEIGHBORS).transform(resource("push-update-interfaces.json")).isEmpty());
    Assertions.assertTrue(transformer(BGP_NEIGHBORS).transform(resource("restconf-change-update.json")).isEmpty());

    final List<ObjectNode> rows = new YangJsonTransformer(ImmutableList.of(BGP_NEIGHBORS), true, true, true, true)
        .transform(resource("push-update-interfaces.json"));
    Assertions.assertEquals(1, rows.size());
    Assertions.assertFalse(rows.get(0).has(YangJsonTransformer.YANG_RULE));
    Assertions.assertFalse(rows.get(0).has("interfaces"));
    Assertions.assertEquals(1011L, rows.get(0).get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    Assertions.assertEquals("2026-09-30T12:00:00.000Z", rows.get(0).get(YangJsonTransformer.EVENT_TIME).textValue());
  }

  @Test
  public void testNoRulesKeepsTree() throws IOException
  {
    final List<ObjectNode> rows = transformer().transform(resource("push-update-interfaces.json"));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(2, rows.get(0).get("interfaces").get("interface").size());
    Assertions.assertEquals(1011L, rows.get(0).get(YangJsonTransformer.SUBSCRIPTION_ID).longValue());
    Assertions.assertFalse(rows.get(0).has(YangJsonTransformer.YANG_RULE));
  }

  @Test
  public void testNoRulesChangeUpdateGivesRowPerEdit() throws IOException
  {
    final List<ObjectNode> rows = transformer().transform(resource("restconf-change-update.json"));

    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(
        "down",
        rows.get(0).get("interfaces").get("interface").get(0).get("oper-status").textValue()
    );
    Assertions.assertEquals("delete", rows.get(1).get(YangJsonTransformer.YANG_OPERATION).textValue());
  }

  @Test
  public void testAllOptionsOff() throws IOException
  {
    final JsonNode document = resource("push-update-interfaces.json");
    final List<ObjectNode> rows = new YangJsonTransformer(ImmutableList.of(), false, false, false, false)
        .transform(document);

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(document, rows.get(0));
  }

  @Test
  public void testExplodePathWithoutStrippingPrefixes() throws IOException
  {
    // Path segments match by local name, so an unprefixed path still finds prefixed members
    final List<ObjectNode> rows = new YangJsonTransformer(ImmutableList.of(INTERFACES), false, true, true, false)
        .transform(resource("push-update-interfaces.json"));

    Assertions.assertEquals(2, rows.size());
    Assertions.assertTrue(rows.get(0).has("ietf-ip:ipv4"));
    Assertions.assertTrue(rows.get(0).get("acme-ext:maintenance-mode").booleanValue());
  }

  @Test
  public void testPrefixCollisionKeepsQualifiedName() throws IOException
  {
    final List<ObjectNode> rows = new YangJsonTransformer(ImmutableList.of(), true, true, false, false).transform(
        MAPPER.readTree("{\"a:counter\": \"1\", \"b:counter\": \"2\"}")
    );

    Assertions.assertEquals(1L, rows.get(0).get("counter").longValue());
    Assertions.assertEquals(2L, rows.get(0).get("b:counter").longValue());
  }

  @Test
  public void testCoercionLeavesNonNumericStringsAlone() throws IOException
  {
    final ObjectNode row = new YangJsonTransformer(ImmutableList.of(), true, true, false, false).transform(
        MAPPER.readTree("{\"a\": \"007\", \"b\": \"-3.25\", \"c\": \"1e5\", \"d\": \"10.0.0.1\", \"e\": [null, null]}")
    ).get(0);

    Assertions.assertEquals("007", row.get("a").textValue());
    Assertions.assertEquals(-3.25, row.get("b").doubleValue(), 0);
    Assertions.assertEquals("1e5", row.get("c").textValue());
    Assertions.assertEquals("10.0.0.1", row.get("d").textValue());
    Assertions.assertTrue(row.get("e").isArray());
  }

  @Test
  public void testTargetPath()
  {
    Assertions.assertEquals(
        ImmutableList.of("interfaces", "interface", "ipv6"),
        YangJsonTransformer.targetPath("/ietf-interfaces:interfaces/interface=eth0%2F1/ietf-ip:ipv6")
    );
  }
}
