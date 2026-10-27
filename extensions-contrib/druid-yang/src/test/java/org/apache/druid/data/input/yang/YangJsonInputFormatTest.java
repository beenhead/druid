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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class YangJsonInputFormatTest
{
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new YangModule().getJacksonModules());
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, mapper));
  }

  private static byte[] resource(String name) throws IOException
  {
    try (InputStream in = YangJsonInputFormatTest.class.getResourceAsStream("/yang/" + name)) {
      return in.readAllBytes();
    }
  }

  @Test
  public void testSerde() throws IOException
  {
    final YangJsonInputFormat format = new YangJsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(new JSONPathFieldSpec(JSONPathFieldType.PATH, "inOctets", "$.statistics.in-octets"))
        ),
        "interfaces/interface",
        null,
        null,
        false,
        null,
        null,
        true,
        null,
        null,
        null,
        null, mapper
    );
    final InputFormat roundTripped = mapper.readValue(mapper.writeValueAsString(format), InputFormat.class);
    Assertions.assertEquals(format, roundTripped);
  }

  @Test
  public void testDefaults() throws IOException
  {
    final YangJsonInputFormat format =
        (YangJsonInputFormat) mapper.readValue("{\"type\": \"yangJson\"}", InputFormat.class);
    Assertions.assertNull(format.getExplodePath());
    Assertions.assertTrue(format.getRules().isEmpty());
    Assertions.assertEquals(YangJsonInputFormat.OnUnmatched.FAIL, format.getOnUnmatched());
    Assertions.assertTrue(format.isStripModulePrefixes());
    Assertions.assertTrue(format.isCoerceTypes());
    Assertions.assertTrue(format.isUnwrapEnvelope());
    Assertions.assertFalse(format.isKeepNullColumns());
  }

  @Test
  public void testReadWithFlattenSpec() throws IOException
  {
    final InputFormat format = mapper.readValue(
        "{\"type\": \"yangJson\","
        + " \"explodePath\": \"interfaces/interface\","
        + " \"flattenSpec\": {\"fields\": ["
        + "   {\"type\": \"path\", \"name\": \"inOctets\", \"expr\": \"$.statistics['in-octets']\"},"
        + "   {\"type\": \"path\", \"name\": \"inErrors\", \"expr\": \"$.statistics['in-errors']\"}"
        + " ]}}",
        InputFormat.class
    );

    final List<InputRow> rows = read(
        format,
        new InputRowSchema(
            new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("name", "oper-status"))),
            ColumnsFilter.all()
        ),
        resource("push-update-interfaces.json")
    );

    Assertions.assertEquals(2, rows.size());
    Assertions.assertEquals(DateTimes.of("2026-09-30T12:00:00.000Z"), rows.get(0).getTimestamp());
    Assertions.assertEquals(ImmutableList.of("eth0"), rows.get(0).getDimension("name"));
    Assertions.assertEquals(ImmutableList.of("up"), rows.get(0).getDimension("oper-status"));
    Assertions.assertEquals(123456789012L, rows.get(0).getMetric("inOctets").longValue());
    Assertions.assertEquals(ImmutableList.of("eth1"), rows.get(1).getDimension("name"));
    Assertions.assertEquals(7L, rows.get(1).getMetric("inErrors").longValue());
  }

  @Test
  public void testReadWithSchemaDiscovery() throws IOException
  {
    final InputFormat format = new YangJsonInputFormat(null, "interfaces/interface", null, null, null, null, null, null, null, null, null, null, mapper);
    final List<InputRow> rows = read(
        format,
        new InputRowSchema(
            new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        resource("push-update-interfaces.json")
    );

    Assertions.assertEquals(2, rows.size());
    final InputRow eth0 = rows.get(0);
    Assertions.assertTrue(eth0.getDimensions().contains("name"));
    Assertions.assertTrue(eth0.getDimensions().contains("statistics"));
    Assertions.assertTrue(eth0.getDimensions().contains(YangJsonTransformer.SUBSCRIPTION_ID));
    Assertions.assertEquals(123456789012L, ((Map<?, ?>) eth0.getRaw("statistics")).get("in-octets"));
  }

  @Test
  public void testSampleReturnsTransformedRows() throws IOException
  {
    final InputFormat format = new YangJsonInputFormat(null, "interfaces/interface", null, null, null, null, null, null, null, null, null, null, mapper);
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        new ByteEntity(resource("push-update-interfaces.json")),
        null
    );

    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      Assertions.assertTrue(iterator.hasNext());
      final InputRowListPlusRawValues sample = iterator.next();
      Assertions.assertNull(sample.getParseException());
      Assertions.assertEquals(2, sample.getInputRows().size());
      Assertions.assertEquals(2, sample.getRawValuesList().size());
      Assertions.assertEquals("eth1", sample.getRawValuesList().get(1).get("name"));
      Assertions.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testMessageWithoutExplodePathIsParseException() throws IOException
  {
    final InputFormat format =
        new YangJsonInputFormat(null, "network-instances/network-instance", null, null, null, null, null, null, null, null, null, null, mapper);
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        new ByteEntity(resource("push-update-interfaces.json")),
        null
    );

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assertions.assertTrue(iterator.hasNext());
      final ParseException e = Assertions.assertThrows(ParseException.class, iterator::next);
      Assertions.assertTrue(e.getMessage().contains("matched no explode rule"));
    }
  }

  @Test
  public void testMalformedOrEmptyMessageIsParseException() throws IOException
  {
    final InputFormat format =
        new YangJsonInputFormat(null, "interfaces/interface", null, null, null, null, null, null, null, null, null, null, mapper);
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        ColumnsFilter.all()
    );

    for (String message : new String[]{"{\"ietf-yang-push:push-update\": ", "   "}) {
      final InputEntityReader reader = format.createReader(schema, new ByteEntity(StringUtils.toUtf8(message)), null);
      try (CloseableIterator<InputRow> iterator = reader.read()) {
        Assertions.assertTrue(iterator.hasNext());
        final ParseException e = Assertions.assertThrows(ParseException.class, iterator::next);
        Assertions.assertTrue(e.getMessage().startsWith("Unable to parse"), e.getMessage());
      }
    }
  }

  @Test
  public void testSerdeWithRules() throws IOException
  {
    final InputFormat format = mapper.readValue(
        "{\"type\": \"yangJson\", \"onUnmatched\": \"emit\", \"rules\": ["
        + " {\"name\": \"interfaces\", \"path\": \"interfaces/interface\"},"
        + " {\"path\": \"network-instances/network-instance\"}]}",
        InputFormat.class
    );
    final YangJsonInputFormat yang = (YangJsonInputFormat) format;
    Assertions.assertEquals(YangJsonInputFormat.OnUnmatched.EMIT, yang.getOnUnmatched());
    Assertions.assertEquals("interfaces", yang.getRules().get(0).getName());
    Assertions.assertEquals("network-instances/network-instance", yang.getRules().get(1).getName());
    Assertions.assertEquals(format, mapper.readValue(mapper.writeValueAsString(format), InputFormat.class));
  }

  @Test
  public void testExplodePathAndRulesAreExclusive()
  {
    Assertions.assertThrows(
        IAE.class,
        () -> new YangJsonInputFormat(
            null,
            "interfaces/interface",
            ImmutableList.of(new YangExplodeRule(null, "interfaces/interface")),
            null, null, null, null, null, null, null, null,
            null, mapper
        )
    );
  }

  @Test
  public void testUnmatchedEmitGivesEnvelopeOnlyRow() throws IOException
  {
    final InputFormat format = new YangJsonInputFormat(
        null,
        null,
        ImmutableList.of(new YangExplodeRule("bgp", "network-instances/network-instance")),
        YangJsonInputFormat.OnUnmatched.EMIT,
        null, null, null, null, null, null, null,
        null, mapper
    );
    final List<InputRow> rows = read(
        format,
        new InputRowSchema(
            new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        resource("push-update-interfaces.json")
    );

    Assertions.assertEquals(1, rows.size());
    Assertions.assertNull(rows.get(0).getRaw(YangJsonTransformer.YANG_RULE));
    Assertions.assertEquals(DateTimes.of("2026-09-30T12:00:00.000Z"), rows.get(0).getTimestamp());
  }

  private static List<InputRow> read(InputFormat format, InputRowSchema schema, byte[] data) throws IOException
  {
    final InputEntityReader reader = format.createReader(schema, new ByteEntity(data), null);
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }
}
