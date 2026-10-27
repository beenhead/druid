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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * All {@code .yang} files in a local directory, compiled once and used for every message regardless of schema id.
 * Useful for batch ingestion, testing, and deployments without a schema registry.
 */
public class DirectoryYangSchemaSource implements YangSchemaSource
{
  public static final String TYPE = "directory";

  private final String path;
  @Nullable
  private volatile YangSchemaIndex schema;

  @JsonCreator
  public DirectoryYangSchemaSource(@JsonProperty("path") String path)
  {
    this.path = Preconditions.checkNotNull(path, "path");
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  @Override
  public YangSchemaIndex getSchema(@Nullable Integer schemaId)
  {
    YangSchemaIndex result = schema;
    if (result == null) {
      synchronized (this) {
        result = schema;
        if (result == null) {
          result = YangSchemaCompiler.compile(readModules(Paths.get(path)));
          schema = result;
        }
      }
    }
    return result;
  }

  @Override
  public boolean requiresSchemaId()
  {
    return false;
  }

  static Map<String, String> readModules(Path directory)
  {
    final Map<String, String> sources = new TreeMap<>();
    try (Stream<Path> files = Files.list(directory)) {
      final List<Path> yangFiles = files.filter(f -> f.getFileName().toString().endsWith(".yang"))
                                        .collect(Collectors.toList());
      for (Path file : yangFiles) {
        sources.put(file.getFileName().toString(), new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
      }
    }
    catch (IOException e) {
      throw new RE(e, "Cannot read YANG modules from [%s]", directory);
    }
    if (sources.isEmpty()) {
      throw new YangSchemaCompiler.YangSchemaException("No .yang files in [%s]", directory);
    }
    return sources;
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
    return path.equals(((DirectoryYangSchemaSource) o).path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(path);
  }

  @Override
  public String toString()
  {
    return "DirectoryYangSchemaSource{path='" + path + "'}";
  }
}
