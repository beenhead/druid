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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A YANG list (or container) to turn into rows, given as a {@code /}-separated path of node names relative to the
 * data tree. Rows produced by a rule carry its name in the {@link YangJsonTransformer#YANG_RULE} column.
 */
public class YangExplodeRule
{
  private final String name;
  private final String path;
  private final List<String> segments;

  @JsonCreator
  public YangExplodeRule(
      @JsonProperty("name") @Nullable String name,
      @JsonProperty("path") String path
  )
  {
    this.segments = parsePath(Preconditions.checkNotNull(path, "path"));
    Preconditions.checkArgument(!segments.isEmpty(), "path must not be empty");
    this.path = path;
    this.name = name == null ? path : name;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getPath()
  {
    return path;
  }

  List<String> getSegments()
  {
    return segments;
  }

  static List<String> parsePath(String path)
  {
    final List<String> segments = new ArrayList<>();
    for (String segment : path.split("/")) {
      if (!segment.isEmpty()) {
        segments.add(segment);
      }
    }
    return segments;
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
    YangExplodeRule that = (YangExplodeRule) o;
    return name.equals(that.name) && path.equals(that.path);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, path);
  }

  @Override
  public String toString()
  {
    return "YangExplodeRule{name='" + name + "', path='" + path + "'}";
  }
}
