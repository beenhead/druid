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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

/**
 * Where the YANG schema for a message comes from.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = DirectoryYangSchemaSource.TYPE, value = DirectoryYangSchemaSource.class),
    @JsonSubTypes.Type(name = RegistryYangSchemaSource.TYPE, value = RegistryYangSchemaSource.class)
})
public interface YangSchemaSource
{
  /**
   * Returns the schema for a message.
   *
   * @param schemaId the schema id carried by the message (e.g. in a Kafka header), or null if it has none
   *
   * @throws YangSchemaCompiler.YangSchemaException if the schema is invalid or the message has no usable id
   * @throws RuntimeException                       if the schema cannot be retrieved (e.g. registry unreachable)
   */
  YangSchemaIndex getSchema(@Nullable Integer schemaId);

  /**
   * Whether {@link #getSchema} needs a schema id, i.e. fails for messages that don't carry one.
   */
  boolean requiresSchemaId();
}
