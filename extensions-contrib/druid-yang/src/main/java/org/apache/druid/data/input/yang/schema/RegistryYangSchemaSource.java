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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Confluent-compatible schema registry holding YANG schemas, as described in draft-ietf-nmop-yang-message-broker-
 * integration and implemented by the network-analytics/yang-kafka-integration schema registry plugin (schema type
 * {@code YANG}; imported modules registered as references).
 *
 * <p>The schema of a message is looked up by the id it carries. Messages without an id use the latest version of
 * {@code subject}, if configured. Compiled schemas are cached by id for the lifetime of the input format.
 */
public class RegistryYangSchemaSource implements YangSchemaSource
{
  public static final String TYPE = "registry";
  static final String SCHEMA_TYPE_YANG = "YANG";
  private static final String ACCEPT = "application/vnd.schemaregistry.v1+json";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);
  private static final long DEFAULT_LATEST_REFRESH_MILLIS = 300_000L;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern MODULE_NAME = Pattern.compile("^\\s*(?:sub)?module\\s+([A-Za-z_][A-Za-z0-9_.-]*)", Pattern.MULTILINE);

  private final String url;
  @Nullable
  private final String subject;
  @Nullable
  private final String username;
  @Nullable
  private final PasswordProvider password;
  private final long latestRefreshMillis;

  private final Map<Integer, YangSchemaIndex> schemasById = new ConcurrentHashMap<>();
  @Nullable
  private volatile Integer latestId;
  private volatile long latestFetchedAt;
  @Nullable
  private volatile HttpClient httpClient;

  @JsonCreator
  public RegistryYangSchemaSource(
      @JsonProperty("url") String url,
      @JsonProperty("subject") @Nullable String subject,
      @JsonProperty("username") @Nullable String username,
      @JsonProperty("password") @Nullable PasswordProvider password,
      @JsonProperty("latestRefreshMillis") @Nullable Long latestRefreshMillis
  )
  {
    Preconditions.checkNotNull(url, "url");
    this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    this.subject = subject;
    this.username = username;
    this.password = password;
    this.latestRefreshMillis = latestRefreshMillis == null ? DEFAULT_LATEST_REFRESH_MILLIS : latestRefreshMillis;
  }

  @JsonProperty
  public String getUrl()
  {
    return url;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSubject()
  {
    return subject;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getUsername()
  {
    return username;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public PasswordProvider getPassword()
  {
    return password;
  }

  @JsonProperty
  public long getLatestRefreshMillis()
  {
    return latestRefreshMillis;
  }

  @Override
  public YangSchemaIndex getSchema(@Nullable Integer schemaId)
  {
    final int id = schemaId != null ? schemaId : latestIdForSubject();
    final YangSchemaIndex cached = schemasById.get(id);
    if (cached != null) {
      return cached;
    }
    // Compile outside computeIfAbsent: it does network I/O. A race compiles twice, which is harmless.
    final YangSchemaIndex compiled = YangSchemaCompiler.compile(fetchSources(id));
    final YangSchemaIndex previous = schemasById.putIfAbsent(id, compiled);
    return previous != null ? previous : compiled;
  }

  @Override
  public boolean requiresSchemaId()
  {
    return subject == null;
  }

  private int latestIdForSubject()
  {
    if (subject == null) {
      throw new YangSchemaCompiler.YangSchemaException(
          "Message carries no schema id and no fallback subject is configured"
      );
    }
    final long now = System.currentTimeMillis();
    Integer id = latestId;
    if (id == null || now - latestFetchedAt > latestRefreshMillis) {
      final JsonNode latest = get(StringUtils.format("/subjects/%s/versions/latest", encode(subject)));
      id = latest.path("id").asInt();
      latestId = id;
      latestFetchedAt = now;
    }
    return id;
  }

  /**
   * Returns the module source of schema {@code id} and, transitively, of all its references, keyed by module name.
   * Augmenting modules are registered as references of the module they augment even though that module doesn't
   * import them, so the reference graph can reach the same module more than once; each module is kept once.
   */
  Map<String, String> fetchSources(int id)
  {
    final JsonNode root = get(StringUtils.format("/schemas/ids/%d", id));
    checkYang(root, "id " + id);

    final Map<String, String> sources = new LinkedHashMap<>();
    final Set<String> seen = new HashSet<>();
    final Deque<JsonNode> pending = new ArrayDeque<>();
    final String rootSchema = root.path("schema").asText();
    sources.put(moduleName(rootSchema, "id-" + id), rootSchema);
    root.path("references").forEach(pending::add);

    while (!pending.isEmpty()) {
      final JsonNode reference = pending.pop();
      final String refSubject = reference.path("subject").asText();
      final String refVersion = reference.path("version").asText();
      if (!seen.add(refSubject + "/" + refVersion)) {
        continue;
      }
      final JsonNode referenced = get(
          StringUtils.format("/subjects/%s/versions/%s", encode(refSubject), encode(refVersion))
      );
      checkYang(referenced, "subject " + refSubject + " version " + refVersion);
      final String schema = referenced.path("schema").asText();
      sources.putIfAbsent(moduleName(schema, reference.path("name").asText(refSubject)), schema);
      referenced.path("references").forEach(pending::add);
    }
    return sources;
  }

  private static String moduleName(String schema, String fallback)
  {
    final Matcher matcher = MODULE_NAME.matcher(schema);
    return matcher.find() ? matcher.group(1) : fallback;
  }

  private static void checkYang(JsonNode schema, String what)
  {
    final String type = schema.path("schemaType").asText("AVRO");
    if (!SCHEMA_TYPE_YANG.equals(type)) {
      throw new YangSchemaCompiler.YangSchemaException("Schema %s has type [%s], not YANG", what, type);
    }
  }

  private JsonNode get(String path)
  {
    final HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url + path))
                                                   .timeout(TIMEOUT)
                                                   .header("Accept", ACCEPT)
                                                   .GET();
    if (username != null) {
      final String secret = password == null ? "" : password.getPassword();
      request.header(
          "Authorization",
          "Basic " + Base64.getEncoder().encodeToString(StringUtils.toUtf8(username + ":" + secret))
      );
    }
    final HttpResponse<String> response;
    try {
      response = client().send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
    catch (IOException e) {
      throw new RE(e, "Cannot reach schema registry at [%s]", url);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RE(e, "Interrupted while calling schema registry at [%s]", url);
    }
    if (response.statusCode() == 404) {
      // Not transient: the message references a schema that does not exist.
      throw new YangSchemaCompiler.YangSchemaException("Schema registry has no [%s]: %s", path, response.body());
    }
    if (response.statusCode() != 200) {
      throw new RE("Schema registry returned HTTP %d for [%s]: %s", response.statusCode(), path, response.body());
    }
    try {
      return MAPPER.readTree(response.body());
    }
    catch (IOException e) {
      throw new RE(e, "Invalid response from schema registry for [%s]", path);
    }
  }

  private HttpClient client()
  {
    HttpClient client = httpClient;
    if (client == null) {
      synchronized (this) {
        client = httpClient;
        if (client == null) {
          client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
          httpClient = client;
        }
      }
    }
    return client;
  }

  private static String encode(String segment)
  {
    return StringUtils.replace(URLEncoder.encode(segment, StandardCharsets.UTF_8), "+", "%20");
  }

  @JsonIgnore
  int cachedSchemaCount()
  {
    return schemasById.size();
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
    RegistryYangSchemaSource that = (RegistryYangSchemaSource) o;
    return latestRefreshMillis == that.latestRefreshMillis
           && url.equals(that.url)
           && Objects.equals(subject, that.subject)
           && Objects.equals(username, that.username)
           && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(url, subject, username, password, latestRefreshMillis);
  }

  @Override
  public String toString()
  {
    return "RegistryYangSchemaSource{url='" + url + "', subject='" + subject + "'}";
  }
}
