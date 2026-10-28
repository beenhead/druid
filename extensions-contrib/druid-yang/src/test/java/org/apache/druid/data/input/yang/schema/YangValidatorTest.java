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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Validates a telemetry message (draft-ietf-nmop-message-broker-telemetry-message) carrying a YANG-Push notification,
 * with one fault at a time, as in the network-analytics YANG Message Broker Consumer test cases.
 */
public class YangValidatorTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MESSAGE = "/ietf-telemetry-message:message";
  private static final String METADATA = MESSAGE + "/telemetry-message-metadata";
  private static final String SUBSCRIPTION = METADATA + "/ietf-yang-push-telemetry-message:yang-push-subscription";
  private static final String ENVELOPE = MESSAGE + "/payload/ietf-yp-notification:envelope";
  private static final String DATASTORE = ENVELOPE + "/contents/ietf-yang-push:push-update/datastore-contents";
  private static final String INTERFACE = DATASTORE + "/ietf-interfaces:interfaces/interface";

  private static YangSchemaIndex schema;

  @BeforeAll
  public static void setUp() throws URISyntaxException
  {
    schema = new DirectoryYangSchemaSource(
        Paths.get(YangValidatorTest.class.getResource("/yang/telemetry-modules").toURI()).toString()
    ).getSchema(null);
  }

  private static ObjectNode message() throws IOException
  {
    try (InputStream in = YangValidatorTest.class.getResourceAsStream("/yang/telemetry-message-validation.json")) {
      return (ObjectNode) MAPPER.readTree(in);
    }
  }

  private static ObjectNode at(ObjectNode document, String pointer)
  {
    return (ObjectNode) document.at(pointer(pointer));
  }

  /**
   * {@code /a/b[0]/c} -> {@code /a/b/0/c}
   */
  private static String pointer(String path)
  {
    return StringUtils.replace(StringUtils.replace(path, "[", "/"), "]", "");
  }

  private static void rename(ObjectNode document, String parent, String from, String to)
  {
    final ObjectNode node = at(document, parent);
    node.set(to, node.remove(from));
  }

  private static void assertOneProblem(JsonNode document, String expected)
  {
    final YangValidator.Result result = YangValidator.validate(document, schema, 10);
    Assertions.assertEquals(1, result.getCount(), result.getProblems().toString());
    Assertions.assertEquals(expected, result.getProblems().get(0));
  }

  @Test
  public void testStructuresAreIndexed()
  {
    Assertions.assertNotNull(schema.topLevel("ietf-telemetry-message:message"));
    Assertions.assertNotNull(schema.topLevel("ietf-yp-notification:envelope"));
    Assertions.assertNull(schema.getRoot().child("message"));
    Assertions.assertTrue(schema.getBrokenModules().isEmpty());

    final YangSchemaIndex.Node metadata = schema.topLevel("message").child("telemetry-message-metadata");
    Assertions.assertTrue(metadata.child("notification-event").isMandatory());
    Assertions.assertFalse(metadata.child("sequence-number").isMandatory());
    Assertions.assertEquals(YangSchemaIndex.Kind.ANYDATA, schema.topLevel("message").child("payload").getKind());
  }

  @Test
  public void testValidMessage() throws IOException
  {
    final YangValidator.Result result = YangValidator.validate(message(), schema, 10);
    Assertions.assertTrue(result.isValid(), result.getProblems().toString());
  }

  @Test
  public void testUnknownStructure() throws IOException
  {
    final ObjectNode document = message();
    rename(document, "", "ietf-telemetry-message:message", "ietf-telemetry-message:messages");
    assertOneProblem(document, "/ietf-telemetry-message:messages: unknown top-level node");
  }

  @Test
  public void testUnknownStructureInAnydata() throws IOException
  {
    final ObjectNode document = message();
    rename(document, MESSAGE + "/payload", "ietf-yp-notification:envelope", "ietf-yp-notification:envelopes");
    assertOneProblem(document, MESSAGE + "/payload/ietf-yp-notification:envelopes: unknown top-level node");
  }

  @Test
  public void testUnknownLeaf() throws IOException
  {
    final ObjectNode document = message();
    rename(document, SUBSCRIPTION, "id", "ids");
    assertOneProblem(document, SUBSCRIPTION + "/ids: unknown node");
  }

  @Test
  public void testUnknownContainerInAnydata() throws IOException
  {
    final ObjectNode document = message();
    rename(document, DATASTORE, "ietf-interfaces:interfaces", "ietf-interfaces:interface");
    assertOneProblem(document, DATASTORE + "/ietf-interfaces:interface: unknown top-level node");
  }

  @Test
  public void testInvalidValue() throws IOException
  {
    final ObjectNode document = message();
    at(document, SUBSCRIPTION).put("id", "a");
    assertOneProblem(document, SUBSCRIPTION + "/id: invalid value \"a\" for type sn:subscription-id");
  }

  @Test
  public void testUint64OutOfRange() throws IOException
  {
    final ObjectNode document = message();
    at(document, INTERFACE + "[0]/statistics").put("in-octets", "18446744073709551616");
    assertOneProblem(
        document,
        INTERFACE + "[0]/statistics/in-octets: value \"18446744073709551616\" is out of range for type yang:counter64"
    );
  }

  @Test
  public void testMissingMandatoryLeaf() throws IOException
  {
    final ObjectNode document = message();
    at(document, METADATA).remove("notification-event");
    assertOneProblem(document, METADATA + ": missing mandatory leaf [notification-event]");
  }

  @Test
  public void testMandatoryLeavesNotEnforcedInDatastoreData() throws IOException
  {
    // A subscription filter may select only part of an entry, e.g. .../interface/statistics.
    final ObjectNode document = message();
    at(document, INTERFACE + "[0]").remove("type");
    at(document, INTERFACE + "[0]/statistics").remove("discontinuity-time");
    final YangValidator.Result result = YangValidator.validate(document, schema, 10);
    Assertions.assertTrue(result.isValid(), result.getProblems().toString());
  }

  @Test
  public void testMissingListKey() throws IOException
  {
    final ObjectNode document = message();
    at(document, INTERFACE + "[1]").remove("name");
    assertOneProblem(document, INTERFACE + "[1]: missing list key [name]");
  }

  @Test
  public void testEnumeration() throws IOException
  {
    final ObjectNode document = message();
    at(document, INTERFACE + "[0]").put("oper-status", "sideways");
    assertOneProblem(document, INTERFACE + "[0]/oper-status: invalid value \"sideways\" for type enumeration");
  }

  @Test
  public void testIdentityRef() throws IOException
  {
    final ObjectNode notDerived = message();
    at(notDerived, INTERFACE + "[0]").put("type", "ietf-datastores:running");
    assertOneProblem(
        notDerived,
        INTERFACE + "[0]/type: identity [ietf-datastores:running] is not derived from the base of identityref"
    );

    final ObjectNode unknown = message();
    at(unknown, INTERFACE + "[0]").put("type", "iana-if-type:warpDrive");
    assertOneProblem(unknown, INTERFACE + "[0]/type: unknown identity [iana-if-type:warpDrive]");
  }

  @Test
  public void testMandatoryLeavesInChoiceNotEnforced()
  {
    // subscription-started's target is a choice of stream (mandatory stream leaf) or, augmented by ietf-yang-push,
    // datastore.
    final ObjectNode document = MAPPER.createObjectNode();
    document.putObject("ietf-subscribed-notifications:subscription-started")
            .put("id", 1)
            .put("ietf-yang-push:datastore", "ietf-datastores:operational")
            .put("ietf-yang-push:datastore-xpath-filter", "/ietf-interfaces:interfaces")
            .putObject("ietf-yang-push:periodic")
            .put("period", 6000);
    final YangValidator.Result result = YangValidator.validate(document, schema, 10);
    Assertions.assertTrue(result.isValid(), result.getProblems().toString());
  }

  @Test
  public void testBitsInUnion()
  {
    // access-operations is a union of "*" and bits; "" sets no bits.
    for (String valid : new String[]{"*", "", "read exec"}) {
      Assertions.assertTrue(YangValidator.validate(nacmRule(valid), schema, 10).isValid(), valid);
    }
    assertOneProblem(
        nacmRule("read fly"),
        "/ietf-netconf-acm:nacm/rule-list[0]/rule[0]/access-operations: invalid value \"read fly\" for type union"
    );
  }

  private static ObjectNode nacmRule(String accessOperations)
  {
    final ObjectNode document = MAPPER.createObjectNode();
    document.putObject("ietf-netconf-acm:nacm")
            .putArray("rule-list")
            .addObject()
            .put("name", "operators")
            .putArray("rule")
            .addObject()
            .put("name", "r1")
            .put("action", "permit")
            .put("access-operations", accessOperations);
    return document;
  }

  @Test
  public void testWrongKindOfNode() throws IOException
  {
    final ObjectNode document = message();
    at(document, INTERFACE + "[0]").set("statistics", TextNode.valueOf("none"));
    assertOneProblem(document, INTERFACE + "[0]/statistics: expected a container (JSON object)");
  }

  @Test
  public void testYangPatchValueIsNotValidated()
  {
    final ObjectNode document = MAPPER.createObjectNode();
    final ObjectNode edit = document.putObject("ietf-yang-push:push-change-update")
                                    .put("id", 1)
                                    .putObject("datastore-changes")
                                    .putObject("yang-patch")
                                    .put("patch-id", "p1")
                                    .putArray("edit")
                                    .addObject()
                                    .put("edit-id", "e1")
                                    .put("operation", "merge")
                                    .put("target", "/ietf-interfaces:interfaces/interface=eth0");
    // Relative to the target, so not instance data of the schema root.
    edit.putObject("value").putArray("ietf-interfaces:interface").addObject().put("name", "eth0").put("bogus", 1);

    final YangValidator.Result result = YangValidator.validate(document, schema, 10);
    Assertions.assertTrue(result.isValid(), result.getProblems().toString());
  }

  @Test
  public void testProblemsAreCappedButCounted() throws IOException
  {
    final ObjectNode document = message();
    final ArrayNode interfaces = (ArrayNode) document.at(pointer(INTERFACE));
    for (JsonNode entry : interfaces) {
      ((ObjectNode) entry).put("oper-status", "sideways").put("bogus", true);
    }
    final YangValidator.Result result = YangValidator.validate(document, schema, 3);
    Assertions.assertFalse(result.isValid());
    Assertions.assertEquals(4, result.getCount());
    Assertions.assertEquals(3, result.getProblems().size());
  }
}
