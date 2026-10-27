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

import org.apache.druid.data.input.InputEntity;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads a header of the Kafka record behind an {@link InputEntity}, without a compile-time dependency on the Kafka
 * indexing service extension (which lives in another class loader).
 *
 * <p>In streaming ingestion the reader's entity is a {@code SettableByteEntity} whose {@code getEntity()} is the
 * current {@code KafkaRecordEntity}, whose {@code getRecord()} is the Kafka {@code ConsumerRecord}. This holds both
 * when {@code yangJson} is the supervisor's input format and when it is the {@code valueFormat} of the {@code kafka}
 * input format. For any other entity (files, Kinesis, ...) there are no headers and null is returned.
 */
public class RecordHeaders
{
  private static final Map<String, Method> METHODS = new ConcurrentHashMap<>();
  private static final Method NONE;

  static {
    try {
      NONE = Object.class.getMethod("hashCode");
    }
    catch (NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private RecordHeaders()
  {
  }

  @Nullable
  public static byte[] lastHeader(InputEntity source, String name)
  {
    try {
      Object entity = source;
      final Method getEntity = method(entity, "getEntity");
      if (getEntity != null) {
        entity = getEntity.invoke(entity);
      }
      final Method getRecord = entity == null ? null : method(entity, "getRecord");
      if (getRecord == null) {
        return null;
      }
      final Object record = getRecord.invoke(entity);
      final Method headersMethod = record == null ? null : method(record, "headers");
      final Object headers = headersMethod == null ? null : headersMethod.invoke(record);
      if (headers == null) {
        return null;
      }
      final Method lastHeader = headers.getClass().getMethod("lastHeader", String.class);
      lastHeader.setAccessible(true);
      final Object header = lastHeader.invoke(headers, name);
      final Method value = header == null ? null : method(header, "value");
      return value == null ? null : (byte[]) value.invoke(header);
    }
    catch (ReflectiveOperationException | RuntimeException e) {
      return null;
    }
  }

  @Nullable
  private static Method method(Object target, String name)
  {
    final Class<?> clazz = target.getClass();
    final Method method = METHODS.computeIfAbsent(clazz.getName() + "#" + name, ignored -> {
      try {
        final Method m = clazz.getMethod(name);
        m.setAccessible(true);
        return m;
      }
      catch (NoSuchMethodException | RuntimeException e) {
        return NONE;
      }
    });
    return method == NONE ? null : method;
  }
}
