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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.data.input.yang.schema.YangSchemaSource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Objects;

/**
 * Input format for YANG instance data encoded as JSON (RFC 7951), such as YANG-Push notifications or RESTCONF
 * responses. See {@link YangJsonTransformer} for what each option does.
 */
@JsonTypeName(YangJsonInputFormat.TYPE_KEY)
public class YangJsonInputFormat extends NestedInputFormat
{
  public static final String TYPE_KEY = "yangJson";

  /**
   * What to do with a message that matches none of the rules.
   */
  public enum OnUnmatched
  {
    /**
     * Report a parse exception; the message counts as unparseable.
     */
    FAIL,
    /**
     * Emit one row with only the envelope fields and no {@link YangJsonTransformer#YANG_RULE}. Pair with a
     * transformSpec filter on that column to count such messages as thrown away.
     */
    EMIT;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(name());
    }

    @JsonCreator
    public static OnUnmatched fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  /**
   * How the schema id is encoded in its header.
   */
  public enum SchemaIdEncoding
  {
    /**
     * A 4-byte big-endian integer, as written by the network-analytics YANG serializers.
     */
    INT32,
    /**
     * Decimal digits, as shown in the draft-ietf-nmop-yang-message-broker-integration examples.
     */
    STRING;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(name());
    }

    @JsonCreator
    public static SchemaIdEncoding fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  /**
   * What to do with a message without a schema id when the schema source needs one.
   */
  public enum OnMissingSchemaId
  {
    /**
     * Report a parse exception.
     */
    FAIL,
    /**
     * Process the message without a schema, typing values heuristically if {@code coerceTypes} is on.
     */
    HEURISTIC;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(name());
    }

    @JsonCreator
    public static OnMissingSchemaId fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  /**
   * Whether to validate messages against their YANG schema.
   */
  public enum Validation
  {
    /**
     * Don't validate; the schema is only used to type values.
     */
    NONE,
    /**
     * Report a parse exception for a message that is not valid instance data of its schema, listing the problems.
     * Requires a schema.
     */
    STRICT;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(name());
    }

    @JsonCreator
    public static Validation fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  public static final String DEFAULT_SCHEMA_ID_HEADER = "schema-id";

  private final ObjectMapper objectMapper;
  @Nullable
  private final YangSchemaSource schema;
  private final String schemaIdHeader;
  private final SchemaIdEncoding schemaIdEncoding;
  private final OnMissingSchemaId onMissingSchemaId;
  private final Validation validation;
  @Nullable
  private final String explodePath;
  private final List<YangExplodeRule> rules;
  private final OnUnmatched onUnmatched;
  private final boolean stripModulePrefixes;
  private final boolean coerceTypes;
  private final boolean unwrapEnvelope;
  private final boolean keepNullColumns;

  @JsonCreator
  public YangJsonInputFormat(
      @JsonProperty("flattenSpec") @Nullable JSONPathSpec flattenSpec,
      @JsonProperty("explodePath") @Nullable String explodePath,
      @JsonProperty("rules") @Nullable List<YangExplodeRule> rules,
      @JsonProperty("onUnmatched") @Nullable OnUnmatched onUnmatched,
      @JsonProperty("stripModulePrefixes") @Nullable Boolean stripModulePrefixes,
      @JsonProperty("coerceTypes") @Nullable Boolean coerceTypes,
      @JsonProperty("unwrapEnvelope") @Nullable Boolean unwrapEnvelope,
      @JsonProperty("keepNullColumns") @Nullable Boolean keepNullColumns,
      @JsonProperty("schema") @Nullable YangSchemaSource schema,
      @JsonProperty("schemaIdHeader") @Nullable String schemaIdHeader,
      @JsonProperty("schemaIdEncoding") @Nullable SchemaIdEncoding schemaIdEncoding,
      @JsonProperty("onMissingSchemaId") @Nullable OnMissingSchemaId onMissingSchemaId,
      @JsonProperty("validation") @Nullable Validation validation,
      @JacksonInject @Json ObjectMapper objectMapper
  )
  {
    super(flattenSpec);
    if (explodePath != null && rules != null && !rules.isEmpty()) {
      throw new IAE("Specify either explodePath or rules, not both");
    }
    if (validation == Validation.STRICT && schema == null) {
      throw new IAE("validation [%s] needs a schema", validation);
    }
    this.explodePath = explodePath;
    this.rules = rules == null ? ImmutableList.of() : ImmutableList.copyOf(rules);
    this.onUnmatched = onUnmatched == null ? OnUnmatched.FAIL : onUnmatched;
    this.stripModulePrefixes = stripModulePrefixes == null || stripModulePrefixes;
    this.coerceTypes = coerceTypes == null || coerceTypes;
    this.unwrapEnvelope = unwrapEnvelope == null || unwrapEnvelope;
    this.keepNullColumns = keepNullColumns != null && keepNullColumns;
    this.schema = schema;
    this.schemaIdHeader = schemaIdHeader == null ? DEFAULT_SCHEMA_ID_HEADER : schemaIdHeader;
    this.schemaIdEncoding = schemaIdEncoding == null ? SchemaIdEncoding.INT32 : schemaIdEncoding;
    this.onMissingSchemaId = onMissingSchemaId == null ? OnMissingSchemaId.FAIL : onMissingSchemaId;
    this.validation = validation == null ? Validation.NONE : validation;
    this.objectMapper = objectMapper;
  }

  /**
   * Shorthand for a single rule named after its path.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExplodePath()
  {
    return explodePath;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<YangExplodeRule> getRules()
  {
    return rules;
  }

  @JsonProperty
  public OnUnmatched getOnUnmatched()
  {
    return onUnmatched;
  }

  @JsonProperty
  public boolean isStripModulePrefixes()
  {
    return stripModulePrefixes;
  }

  @JsonProperty
  public boolean isCoerceTypes()
  {
    return coerceTypes;
  }

  @JsonProperty
  public boolean isUnwrapEnvelope()
  {
    return unwrapEnvelope;
  }

  @JsonProperty
  public boolean isKeepNullColumns()
  {
    return keepNullColumns;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public YangSchemaSource getSchema()
  {
    return schema;
  }

  @JsonProperty
  public String getSchemaIdHeader()
  {
    return schemaIdHeader;
  }

  @JsonProperty
  public SchemaIdEncoding getSchemaIdEncoding()
  {
    return schemaIdEncoding;
  }

  @JsonProperty
  public OnMissingSchemaId getOnMissingSchemaId()
  {
    return onMissingSchemaId;
  }

  @JsonProperty
  public Validation getValidation()
  {
    return validation;
  }

  List<YangExplodeRule> getEffectiveRules()
  {
    return explodePath == null ? rules : ImmutableList.of(new YangExplodeRule(null, explodePath));
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new YangJsonNodeReader(
        inputRowSchema,
        source,
        getFlattenSpec(),
        objectMapper,
        keepNullColumns,
        new YangJsonTransformer(
            getEffectiveRules(),
            stripModulePrefixes,
            coerceTypes,
            unwrapEnvelope,
            onUnmatched == OnUnmatched.EMIT
        ),
        schema,
        schemaIdHeader,
        schemaIdEncoding,
        onMissingSchemaId,
        validation
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    YangJsonInputFormat that = (YangJsonInputFormat) o;
    return stripModulePrefixes == that.stripModulePrefixes
           && coerceTypes == that.coerceTypes
           && unwrapEnvelope == that.unwrapEnvelope
           && keepNullColumns == that.keepNullColumns
           && onUnmatched == that.onUnmatched
           && schemaIdEncoding == that.schemaIdEncoding
           && onMissingSchemaId == that.onMissingSchemaId
           && validation == that.validation
           && schemaIdHeader.equals(that.schemaIdHeader)
           && Objects.equals(schema, that.schema)
           && Objects.equals(explodePath, that.explodePath)
           && rules.equals(that.rules);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        explodePath,
        rules,
        onUnmatched,
        stripModulePrefixes,
        coerceTypes,
        unwrapEnvelope,
        keepNullColumns,
        schema,
        schemaIdHeader,
        schemaIdEncoding,
        onMissingSchemaId,
        validation
    );
  }

  @Override
  public String toString()
  {
    return "YangJsonInputFormat{" +
           "flattenSpec=" + getFlattenSpec() +
           ", explodePath='" + explodePath + '\'' +
           ", rules=" + rules +
           ", onUnmatched=" + onUnmatched +
           ", stripModulePrefixes=" + stripModulePrefixes +
           ", coerceTypes=" + coerceTypes +
           ", unwrapEnvelope=" + unwrapEnvelope +
           ", keepNullColumns=" + keepNullColumns +
           ", schema=" + schema +
           ", schemaIdHeader='" + schemaIdHeader + '\'' +
           ", schemaIdEncoding=" + schemaIdEncoding +
           ", onMissingSchemaId=" + onMissingSchemaId +
           ", validation=" + validation +
           '}';
  }
}
