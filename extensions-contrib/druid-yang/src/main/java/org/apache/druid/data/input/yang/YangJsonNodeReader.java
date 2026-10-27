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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.yang.schema.RecordHeaders;
import org.apache.druid.data.input.yang.schema.YangSchemaCompiler;
import org.apache.druid.data.input.yang.schema.YangSchemaIndex;
import org.apache.druid.data.input.yang.schema.YangSchemaSource;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONFlattenerMaker;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads JSON documents like {@link org.apache.druid.data.input.impl.JsonNodeReader}, then runs each through a
 * {@link YangJsonTransformer}, which may produce any number of rows per document. The optional flattenSpec is applied
 * to each transformed row.
 *
 * <p>Splitting the input into documents follows {@code JsonNodeReader}, which can't be subclassed from outside its
 * package: one or more JSON objects, separated by whitespace; objects before invalid syntax are kept, and parsing stops
 * there.
 */
public class YangJsonNodeReader extends IntermediateRowParsingReader<JsonNode>
{
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final InputRowSchema inputRowSchema;
  private final ObjectMapper mapper;
  private final JsonFactory jsonFactory;
  private final ObjectFlattener<JsonNode> flattener;
  private final YangJsonTransformer transformer;
  private final InputEntity source;
  @Nullable
  private final YangSchemaSource schemaSource;
  private final String schemaIdHeader;
  private final YangJsonInputFormat.SchemaIdEncoding schemaIdEncoding;
  private final YangJsonInputFormat.OnMissingSchemaId onMissingSchemaId;

  public YangJsonNodeReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      @Nullable JSONPathSpec flattenSpec,
      ObjectMapper mapper,
      boolean keepNullColumns,
      YangJsonTransformer transformer,
      @Nullable YangSchemaSource schemaSource,
      String schemaIdHeader,
      YangJsonInputFormat.SchemaIdEncoding schemaIdEncoding,
      YangJsonInputFormat.OnMissingSchemaId onMissingSchemaId
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.mapper = mapper;
    this.jsonFactory = new JsonFactory();
    this.flattener = ObjectFlatteners.create(
        flattenSpec,
        new JSONFlattenerMaker(keepNullColumns, inputRowSchema.getDimensionsSpec().useSchemaDiscovery())
    );
    this.transformer = transformer;
    this.source = source;
    this.schemaSource = schemaSource;
    this.schemaIdHeader = schemaIdHeader;
    this.schemaIdEncoding = schemaIdEncoding;
    this.onMissingSchemaId = onMissingSchemaId;
  }

  @Override
  protected CloseableIterator<JsonNode> intermediateRowIterator() throws IOException
  {
    final List<JsonNode> jsonNodes = new ArrayList<>();
    try (final JsonParser parser = jsonFactory.createParser(source.open())) {
      final MappingIterator<JsonNode> delegate = mapper.readValues(parser, JsonNode.class);
      while (delegate.hasNext()) {
        jsonNodes.add(delegate.next());
      }
    }
    catch (Exception e) {
      // MappingIterator wraps Jackson's JsonParseException when the text is ill-formed; report it as a parse exception.
      if (e.getCause() instanceof JsonParseException) {
        final String rowAsString = IOUtils.toString(source.open(), StandardCharsets.UTF_8);
        jsonNodes.add(
            new ParseExceptionMarkerJsonNode(
                new ParseException(rowAsString, e, "Unable to parse row [%s]", rowAsString)
            )
        );
      } else {
        throw e;
      }
    }

    if (jsonNodes.isEmpty()) {
      final String rowAsString = IOUtils.toString(source.open(), StandardCharsets.UTF_8);
      jsonNodes.add(
          new ParseExceptionMarkerJsonNode(
              new ParseException(
                  rowAsString,
                  "Unable to parse [%s] as the intermediateRow resulted in empty input row",
                  rowAsString
              )
          )
      );
    }
    return CloseableIterators.withEmptyBaggage(jsonNodes.iterator());
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(JsonNode intermediateRow) throws ParseException
  {
    if (intermediateRow instanceof ParseExceptionMarkerJsonNode) {
      throw ((ParseExceptionMarkerJsonNode) intermediateRow).getParseException();
    }
    final List<ObjectNode> transformed = transformer.transform(intermediateRow, resolveSchema(intermediateRow));
    if (transformed.isEmpty()) {
      // IntermediateRowParsingReader requires at least one row per intermediate row, so a message that matches no
      // rule (e.g. another subscription on the same topic) must be reported, not skipped. With onUnmatched=emit the
      // transformer returns an envelope-only row instead, which a transformSpec filter can count as thrown away.
      throw new ParseException(
          intermediateRow.toString(),
          "Message matched no explode rule: [%s]",
          intermediateRow.toString()
      );
    }
    final List<InputRow> rows = new ArrayList<>(transformed.size());
    for (ObjectNode node : transformed) {
      rows.add(MapInputRowParser.parse(inputRowSchema, flattener.flatten(node)));
    }
    return rows;
  }

  /**
   * Looks up the schema for the current message. Problems with the message itself (unknown schema id, invalid
   * schema) are parse exceptions; failures to reach the registry are not, so the task fails and retries instead of
   * discarding data.
   */
  @Nullable
  private YangSchemaIndex resolveSchema(JsonNode intermediateRow)
  {
    if (schemaSource == null) {
      return null;
    }
    try {
      final Integer id = schemaId();
      if (id == null
          && schemaSource.requiresSchemaId()
          && onMissingSchemaId == YangJsonInputFormat.OnMissingSchemaId.HEURISTIC) {
        return null;
      }
      return schemaSource.getSchema(id);
    }
    catch (YangSchemaCompiler.YangSchemaException e) {
      throw new ParseException(intermediateRow.toString(), e, "Cannot resolve YANG schema: %s", e.getMessage());
    }
  }

  @Nullable
  private Integer schemaId()
  {
    final byte[] header = RecordHeaders.lastHeader(source, schemaIdHeader);
    if (header == null) {
      return null;
    }
    if (schemaIdEncoding == YangJsonInputFormat.SchemaIdEncoding.INT32) {
      if (header.length != Integer.BYTES) {
        throw new YangSchemaCompiler.YangSchemaException(
            "Header [%s] has %d bytes, expected a 4-byte schema id",
            schemaIdHeader,
            header.length
        );
      }
      return ByteBuffer.wrap(header).getInt();
    }
    final String text = new String(header, StandardCharsets.UTF_8).trim();
    try {
      return Integer.parseInt(text);
    }
    catch (NumberFormatException e) {
      throw new YangSchemaCompiler.YangSchemaException("Header [%s] is not a schema id: [%s]", schemaIdHeader, text);
    }
  }

  @Override
  protected List<Map<String, Object>> toMap(JsonNode intermediateRow)
  {
    if (intermediateRow instanceof ParseExceptionMarkerJsonNode) {
      throw ((ParseExceptionMarkerJsonNode) intermediateRow).getParseException();
    }
    final List<ObjectNode> transformed = transformer.transform(intermediateRow, resolveSchema(intermediateRow));
    final List<Map<String, Object>> maps = new ArrayList<>(transformed.size());
    for (ObjectNode node : transformed) {
      maps.add(mapper.convertValue(node, MAP_TYPE));
    }
    return maps;
  }

  /**
   * Carries a parse error from {@link #intermediateRowIterator()} to {@link #parseInputRows}, where it can be thrown.
   */
  private static class ParseExceptionMarkerJsonNode extends ObjectNode
  {
    private final ParseException parseException;

    ParseExceptionMarkerJsonNode(ParseException pe)
    {
      super(null);
      this.parseException = pe;
    }

    ParseException getParseException()
    {
      return parseException;
    }
  }
}
