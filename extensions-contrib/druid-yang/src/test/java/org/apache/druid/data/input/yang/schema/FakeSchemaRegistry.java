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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal Confluent schema registry REST API serving YANG schemas the way the network-analytics
 * yang-schema-registry-plugin stores them: one subject per module, imports registered as references.
 *
 * <pre>
 * id 7  acme-if-ext v1      refs: ietf-interfaces v1, ietf-ip v1
 * id 2  ietf-interfaces v1  refs: ietf-yang-types v1
 * id 3  ietf-ip v1          refs: ietf-interfaces v1, ietf-inet-types v1, ietf-yang-types v1
 * id 4  ietf-yang-types v1
 * id 5  ietf-inet-types v1
 * id 8  interfaces-push     ietf-interfaces again; refs: ietf-yang-types v1, ietf-ip v1, acme-if-ext v1
 * id 9  an AVRO schema
 * </pre>
 */
public class FakeSchemaRegistry implements AutoCloseable
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpServer server;
  private final Map<String, ObjectNode> byPath = new ConcurrentHashMap<>();
  private final AtomicInteger requests = new AtomicInteger();

  public FakeSchemaRegistry() throws IOException
  {
    final Map<String, String> modules = YangSchemaCompilerTest.modules();
    subject(4, "ietf-yang-types", modules.get("ietf-yang-types@2013-07-15.yang"));
    subject(5, "ietf-inet-types", modules.get("ietf-inet-types@2013-07-15.yang"));
    subject(2, "ietf-interfaces", modules.get("ietf-interfaces@2018-02-20.yang"), "ietf-yang-types");
    subject(3, "ietf-ip", modules.get("ietf-ip@2018-02-22.yang"), "ietf-interfaces", "ietf-inet-types", "ietf-yang-types");
    subject(7, "acme-if-ext", modules.get("acme-if-ext@2026-09-30.yang"), "ietf-interfaces", "ietf-ip");
    // As in the draft: the subscribed module, with its augmenting modules listed as references too.
    subject(
        8,
        "interfaces-push",
        modules.get("ietf-interfaces@2018-02-20.yang"),
        "ietf-yang-types",
        "ietf-ip",
        "acme-if-ext"
    );

    final ObjectNode avro = MAPPER.createObjectNode();
    avro.put("schema", "{\"type\": \"string\"}");
    byPath.put("/schemas/ids/9", avro);

    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", exchange -> {
      requests.incrementAndGet();
      final String path = URLDecoder.decode(exchange.getRequestURI().getRawPath(), StandardCharsets.UTF_8);
      final ObjectNode body = byPath.get(path);
      final byte[] bytes;
      if (body == null) {
        bytes = StringUtils.toUtf8("{\"error_code\": 40403, \"message\": \"Schema not found\"}");
        exchange.sendResponseHeaders(404, bytes.length);
      } else {
        bytes = MAPPER.writeValueAsBytes(body);
        exchange.sendResponseHeaders(200, bytes.length);
      }
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    });
    server.start();
  }

  private void subject(int id, String subject, String schema, String... references)
  {
    final ObjectNode node = MAPPER.createObjectNode();
    node.put("subject", subject);
    node.put("version", 1);
    node.put("id", id);
    node.put("schemaType", "YANG");
    node.put("schema", schema);
    final ArrayNode refs = node.putArray("references");
    for (String reference : references) {
      refs.addObject().put("name", reference).put("subject", reference).put("version", 1);
    }
    byPath.put("/schemas/ids/" + id, node);
    byPath.put("/subjects/" + subject + "/versions/1", node);
    byPath.put("/subjects/" + subject + "/versions/latest", node);
  }

  public String url()
  {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  public int requestCount()
  {
    return requests.get();
  }

  @Override
  public void close()
  {
    server.stop(0);
  }
}
