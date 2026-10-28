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
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.yang.schema.FakeSchemaRegistry;
import org.apache.druid.data.input.yang.schema.RegistryYangSchemaSource;
import org.apache.druid.data.input.yang.schema.YangSchemaSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads messages with a schema id in a Kafka header, as produced by the network-analytics YANG JSON serializer.
 */
public class YangSchemaReaderTest
{
  private FakeSchemaRegistry registry;
  private ObjectMapper mapper;

  @BeforeEach
  public void setUp() throws IOException
  {
    registry = new FakeSchemaRegistry();
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new YangModule().getJacksonModules());
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, mapper));
  }

  @AfterEach
  public void tearDown()
  {
    registry.close();
  }

  private static byte[] message() throws IOException
  {
    try (InputStream in = YangSchemaReaderTest.class.getResourceAsStream("/yang/schema-typed-interfaces.json")) {
      return in.readAllBytes();
    }
  }

  private InputFormat format(@Nullable String subject, @Nullable String encoding) throws IOException
  {
    return mapper.readValue(
        StringUtils.format(
            "{\"type\": \"yangJson\", \"explodePath\": \"interfaces/interface\","
            + " \"schema\": {\"type\": \"registry\", \"url\": \"%s\"%s}%s}",
            registry.url(),
            subject == null ? "" : ", \"subject\": \"" + subject + "\"",
            encoding == null ? "" : ", \"schemaIdEncoding\": \"" + encoding + "\""
        ),
        InputFormat.class
    );
  }

  private static List<InputRow> read(InputFormat format, InputEntity entity) throws IOException
  {
    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec(YangJsonTransformer.EVENT_TIME, "iso", null),
            DimensionsSpec.builder().useSchemaDiscovery(true).build(),
            ColumnsFilter.all()
        ),
        entity,
        null
    );
    final List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static InputEntity kafka(@Nullable byte[] schemaIdHeader) throws IOException
  {
    // Streaming ingestion hands readers a settable wrapper around the current record entity.
    return new FakeSettableEntity(new FakeKafkaRecordEntity(message(), schemaIdHeader));
  }

  private static byte[] int32(int id)
  {
    return ByteBuffer.allocate(Integer.BYTES).putInt(id).array();
  }

  @Test
  public void testSchemaIdFromInt32Header() throws IOException
  {
    final List<InputRow> rows = read(format(null, null), kafka(int32(7)));

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("42", rows.get(0).getRaw("description"));
    Assertions.assertEquals(123456789012L, ((Map<?, ?>) rows.get(0).getRaw("statistics")).get("in-octets"));
    Assertions.assertEquals("12345", ((Map<?, ?>) rows.get(0).getRaw("optics")).get("serial-number"));
  }

  @Test
  public void testSchemaIdFromStringHeader() throws IOException
  {
    final List<InputRow> rows = read(format(null, "string"), kafka(StringUtils.toUtf8("7")));
    Assertions.assertEquals("42", rows.get(0).getRaw("description"));
  }

  @Test
  public void testNoHeaderUsesSubjectOrFails() throws IOException
  {
    Assertions.assertEquals("42", read(format("acme-if-ext", null), kafka(null)).get(0).getRaw("description"));
    // a plain entity (no Kafka record behind it) behaves like a missing header
    Assertions.assertEquals(
        "42",
        read(format("acme-if-ext", null), new ByteEntity(message())).get(0).getRaw("description")
    );
    Assertions.assertThrows(ParseException.class, () -> read(format(null, null), kafka(null)));
  }

  @Test
  public void testBadHeaderOrUnknownIdIsParseException() throws IOException
  {
    Assertions.assertThrows(ParseException.class, () -> read(format(null, null), kafka(new byte[]{1, 2})));
    Assertions.assertThrows(ParseException.class, () -> read(format(null, null), kafka(int32(99))));
    Assertions.assertThrows(ParseException.class, () -> read(format(null, "string"), kafka(StringUtils.toUtf8("x"))));
  }

  @Test
  public void testMissingIdCanFallBackToHeuristic() throws IOException
  {
    final InputFormat format = mapper.readValue(
        StringUtils.format(
            "{\"type\": \"yangJson\", \"explodePath\": \"interfaces/interface\","
            + " \"schema\": {\"type\": \"registry\", \"url\": \"%s\"}, \"onMissingSchemaId\": \"heuristic\"}",
            registry.url()
        ),
        InputFormat.class
    );
    // no header: typed heuristically instead of failing
    Assertions.assertEquals(42L, read(format, kafka(null)).get(0).getRaw("description"));
    // with a header the schema still applies
    Assertions.assertEquals("42", read(format, kafka(int32(7))).get(0).getRaw("description"));
    Assertions.assertEquals(
        YangJsonInputFormat.OnMissingSchemaId.HEURISTIC,
        ((YangJsonInputFormat) format).getOnMissingSchemaId()
    );
    Assertions.assertEquals(format, mapper.readValue(mapper.writeValueAsString(format), InputFormat.class));
  }

  @Test
  public void testFormatSerde() throws IOException
  {
    final InputFormat format = format("acme-if-ext", "string");
    Assertions.assertEquals(format, mapper.readValue(mapper.writeValueAsString(format), InputFormat.class));
    final YangSchemaSource source = ((YangJsonInputFormat) format).getSchema();
    Assertions.assertTrue(source instanceof RegistryYangSchemaSource);
    Assertions.assertEquals("schema-id", ((YangJsonInputFormat) format).getSchemaIdHeader());
  }

  @Test
  public void testWithoutSchemaSameMessageIsTypedHeuristically() throws IOException
  {
    final InputFormat format =
        new YangJsonInputFormat(null, "interfaces/interface", null, null, null, null, null, null, null, null, null, null, null, mapper);
    final List<InputRow> rows = read(format, kafka(int32(7)));
    Assertions.assertEquals(42L, rows.get(0).getRaw("description"));
  }

  /**
   * Mirrors Druid's SettableByteEntity: {@code getEntity()} returns the current record entity.
   */
  public static class FakeSettableEntity implements InputEntity
  {
    private final FakeKafkaRecordEntity entity;

    FakeSettableEntity(FakeKafkaRecordEntity entity)
    {
      this.entity = entity;
    }

    public FakeKafkaRecordEntity getEntity()
    {
      return entity;
    }

    @Nullable
    @Override
    public java.net.URI getUri()
    {
      return null;
    }

    @Override
    public InputStream open() throws IOException
    {
      return entity.open();
    }

    public InputStream openRaw() throws IOException
    {
      return entity.open();
    }
  }

  /**
   * Mirrors KafkaRecordEntity: {@code getRecord().headers().lastHeader(name).value()}.
   */
  public static class FakeKafkaRecordEntity extends ByteEntity
  {
    private final FakeRecord record;

    FakeKafkaRecordEntity(byte[] value, @Nullable byte[] schemaIdHeader)
    {
      super(value);
      this.record = new FakeRecord(schemaIdHeader == null ? ImmutableList.of() : ImmutableList.of(schemaIdHeader));
    }

    public FakeRecord getRecord()
    {
      return record;
    }
  }

  public static class FakeRecord
  {
    private final FakeHeaders headers;

    FakeRecord(List<byte[]> schemaIds)
    {
      this.headers = new FakeHeaders(schemaIds);
    }

    public FakeHeaders headers()
    {
      return headers;
    }
  }

  public static class FakeHeaders
  {
    private final List<byte[]> schemaIds;

    FakeHeaders(List<byte[]> schemaIds)
    {
      this.schemaIds = schemaIds;
    }

    @Nullable
    public FakeHeader lastHeader(String key)
    {
      return "schema-id".equals(key) && !schemaIds.isEmpty() ? new FakeHeader(schemaIds.get(schemaIds.size() - 1)) : null;
    }
  }

  public static class FakeHeader
  {
    private final byte[] value;

    FakeHeader(byte[] value)
    {
      this.value = value;
    }

    public byte[] value()
    {
      return value;
    }
  }
}
