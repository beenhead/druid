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

/**
 * draft-ietf-nmop-network-incident-yang-17 ({@code ietf-incident}): incidents arrive as a YANG notification, not as
 * datastore contents, and embed events that carry their own notification envelope.
 */
public class YangIncidentTest
{
  private static final String INCIDENT = "incident-notification";
  private static YangSchemaIndex schema;

  @BeforeAll
  public static void setUp() throws URISyntaxException
  {
    schema = new DirectoryYangSchemaSource(
        Paths.get(YangIncidentTest.class.getResource("/yang/incident-modules").toURI()).toString()
    ).getSchema(null);
  }

  private static List<ObjectNode> rows(String path) throws IOException
  {
    return new YangJsonTransformer(ImmutableList.of(new YangExplodeRule(null, path)), true, true, true, false)
        .transform(YangJsonTransformerTest.resource("incident-notification.json"), schema);
  }

  @Test
  public void testNotificationsAreNamedContainersAndNotFlattened()
  {
    final YangSchemaIndex.Node root = schema.getRoot();

    final YangSchemaIndex.Node notification = root.child(INCIDENT);
    Assertions.assertNotNull(notification);
    Assertions.assertEquals(YangSchemaIndex.Kind.CONTAINER, notification.getKind());
    Assertions.assertEquals(YangSchemaIndex.ValueType.INTEGER_EXACT, notification.child("incident-no").getValueType());
    Assertions.assertEquals(YangSchemaIndex.ValueType.OTHER, notification.child("incident-qualifier").getValueType());
    Assertions.assertEquals(ImmutableList.of("node-ref", "cause-name"), notification.child("probable-causes").child("probable-cause").getKeys());
    Assertions.assertNotNull(root.child("alarm-notification"));

    // choice/case are transparent: each case's container sits directly under event; anydata has no schema below it
    final YangSchemaIndex.Node event = notification.child("events").child("event");
    Assertions.assertEquals(ImmutableList.of("type", "event-id"), event.getKeys());
    Assertions.assertNull(event.child("event-type-info"));
    Assertions.assertEquals(YangSchemaIndex.ValueType.DECIMAL, event.child("metric").child("observed-value").getValueType());
    Assertions.assertNotNull(event.child("alarm").child("alarm-type-id"));
    Assertions.assertEquals(
        YangSchemaIndex.ValueType.INTEGER,
        event.child("notification").child("sequence-number").getValueType()
    );
    Assertions.assertEquals(YangSchemaIndex.Kind.ANYDATA, event.child("notification").child("contents").getKind());
    Assertions.assertTrue(event.child("notification").child("contents").getChildren().isEmpty());

    // the contents of notifications don't leak into the top level, and rpcs and sx:structure aren't instance data
    for (String name : new String[]{"incident-no", "status", "priority", "perceived-severity", "alarm-text",
                                    "incident-acknowledge", "incident-acknowledge-error-info"}) {
      Assertions.assertNull(root.child(name), name);
    }

    // datastore data is unchanged
    final YangSchemaIndex.Node incident = root.child("incidents").child("incident");
    Assertions.assertEquals(ImmutableList.of("name", "type", "incident-qualifier"), incident.getKeys());
    Assertions.assertNotNull(root.child("alarms").child("alarm-list").child("alarm"));
    Assertions.assertNotNull(root.child("networks").child("network").child("node"));
  }

  @Test
  public void testIncidentRowIsSchemaTyped() throws IOException
  {
    final List<ObjectNode> rows = rows(INCIDENT);

    Assertions.assertEquals(1, rows.size());
    final ObjectNode row = rows.get(0);
    Assertions.assertEquals("high", row.get("priority").textValue());
    Assertions.assertEquals("raised", row.get("status").textValue());
    // a string key that looks numeric stays a string
    Assertions.assertEquals("42", row.get("incident-qualifier").textValue());
    // a uint64 serial number beyond the long range stays exact
    Assertions.assertEquals("18446744073709550001", row.get("incident-no").textValue());
    Assertions.assertEquals("controller-1.lab.example.net", row.get("hostname").textValue());
  }

  @Test
  public void testChildRowsCarryTheirIncident() throws IOException
  {
    final List<ObjectNode> causes = rows(INCIDENT + "/probable-causes/probable-cause");
    Assertions.assertEquals(1, causes.size());
    Assertions.assertEquals("ietf-incident:loss-of-signal", causes.get(0).get("cause-name").textValue());
    Assertions.assertEquals("18446744073709550001", causes.get(0).get(INCIDENT + ".incident-no").textValue());
    Assertions.assertEquals("high", causes.get(0).get(INCIDENT + ".priority").textValue());

    final List<ObjectNode> events = rows(INCIDENT + "/events/event");
    Assertions.assertEquals(3, events.size());
    for (ObjectNode event : events) {
      Assertions.assertEquals("raised", event.get(INCIDENT + ".status").textValue());
    }
  }

  @Test
  public void testEventRowsAreSchemaTyped() throws IOException
  {
    final List<ObjectNode> events = rows(INCIDENT + "/events/event");

    final ObjectNode metric = events.get(1);
    Assertions.assertEquals("m-1", metric.get("event-id").textValue());
    Assertions.assertEquals(-32.1, metric.get("metric").get("observed-value").doubleValue(), 0);
    Assertions.assertEquals(-12.0, metric.get("metric").get("threshold-value").doubleValue(), 0);
  }

  @Test
  public void testEmbeddedNotificationKeepsPublisherIdentity() throws IOException
  {
    // Exploding down to the embedded notification puts its envelope-like leaves at the top of the row
    final List<ObjectNode> embedded = rows(INCIDENT + "/events/event/notification");

    Assertions.assertEquals(1, embedded.size());
    final ObjectNode row = embedded.get(0);
    Assertions.assertEquals("n-1", row.get("event.event-id").textValue());
    Assertions.assertEquals("18446744073709550001", row.get(INCIDENT + ".incident-no").textValue());

    // the embedded notification's own fields are data; the incident publisher's are kept beside them
    Assertions.assertEquals("router-008", row.get("hostname").textValue());
    Assertions.assertEquals(5501, row.get("sequence-number").intValue());
    Assertions.assertEquals("controller-1.lab.example.net", row.get(YangJsonTransformer.ENVELOPE_PREFIX + "hostname").textValue());
    Assertions.assertEquals(812, row.get(YangJsonTransformer.ENVELOPE_PREFIX + "sequence-number").intValue());
    Assertions.assertEquals("2026-10-01T10:30:00.000Z", row.get(YangJsonTransformer.EVENT_TIME).textValue());
    Assertions.assertEquals("2026-10-01T10:29:42.000Z", row.get("event-time").textValue());

    // anydata has no schema: its content keeps the heuristic and prefix stripping
    Assertions.assertEquals("down", row.get("contents").get("interface").get("oper-status").textValue());

    // no clash, no envelope copy
    Assertions.assertFalse(rows(INCIDENT).get(0).has(YangJsonTransformer.ENVELOPE_PREFIX + "hostname"));
  }
}
