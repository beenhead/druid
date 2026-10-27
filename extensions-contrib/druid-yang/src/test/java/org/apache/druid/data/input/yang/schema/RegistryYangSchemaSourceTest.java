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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

public class RegistryYangSchemaSourceTest
{
  private FakeSchemaRegistry registry;

  @BeforeEach
  public void setUp() throws IOException
  {
    registry = new FakeSchemaRegistry();
  }

  @AfterEach
  public void tearDown()
  {
    registry.close();
  }

  @Test
  public void testFetchesReferencesTransitivelyOnce()
  {
    final RegistryYangSchemaSource source = new RegistryYangSchemaSource(registry.url(), null, null, null, null);
    final Map<String, String> sources = source.fetchSources(7);

    // acme-if-ext plus ietf-interfaces, ietf-ip, ietf-yang-types, ietf-inet-types; shared references fetched once
    Assertions.assertEquals(5, sources.size());
    Assertions.assertTrue(sources.containsKey("ietf-inet-types"));
    Assertions.assertEquals(5, registry.requestCount());
  }

  @Test
  public void testSubscribedModuleWithAugmentsAsReferences()
  {
    final RegistryYangSchemaSource source = new RegistryYangSchemaSource(registry.url(), null, null, null, null);
    final Map<String, String> sources = source.fetchSources(8);

    // ietf-interfaces is both the root and a reference of ietf-ip and acme-if-ext; it's kept once
    Assertions.assertEquals(5, sources.size());
    Assertions.assertTrue(sources.keySet().containsAll(
        java.util.Arrays.asList("ietf-interfaces", "ietf-ip", "acme-if-ext", "ietf-yang-types", "ietf-inet-types")
    ));
    final YangSchemaIndex.Node iface = source.getSchema(8).getRoot().child("interfaces").child("interface");
    Assertions.assertNotNull(iface.child("optics"));
    Assertions.assertNotNull(iface.child("ipv4"));
  }

  @Test
  public void testSchemaIsCompiledAndCachedById()
  {
    final RegistryYangSchemaSource source = new RegistryYangSchemaSource(registry.url(), null, null, null, null);
    final YangSchemaIndex index = source.getSchema(7);

    final YangSchemaIndex.Node iface = index.getRoot().child("interfaces").child("interface");
    Assertions.assertNotNull(iface.child("optics"));
    Assertions.assertNotNull(iface.child("ipv6"));

    final int requests = registry.requestCount();
    Assertions.assertSame(index, source.getSchema(7));
    Assertions.assertEquals(requests, registry.requestCount());
    Assertions.assertEquals(1, source.cachedSchemaCount());
  }

  @Test
  public void testFallbackSubjectWhenMessageHasNoId()
  {
    final RegistryYangSchemaSource source =
        new RegistryYangSchemaSource(registry.url(), "acme-if-ext", null, null, null);
    Assertions.assertNotNull(source.getSchema(null).getRoot().child("interfaces"));

    final RegistryYangSchemaSource noSubject = new RegistryYangSchemaSource(registry.url(), null, null, null, null);
    Assertions.assertThrows(YangSchemaCompiler.YangSchemaException.class, () -> noSubject.getSchema(null));
  }

  @Test
  public void testUnknownIdAndNonYangSchema()
  {
    final RegistryYangSchemaSource source = new RegistryYangSchemaSource(registry.url(), null, null, null, null);
    Assertions.assertThrows(YangSchemaCompiler.YangSchemaException.class, () -> source.getSchema(99));
    Assertions.assertThrows(YangSchemaCompiler.YangSchemaException.class, () -> source.getSchema(9));
  }

  @Test
  public void testUnreachableRegistryIsNotASchemaError()
  {
    final String url = registry.url();
    registry.close();
    final RegistryYangSchemaSource source = new RegistryYangSchemaSource(url, null, null, null, null);
    final RuntimeException e = Assertions.assertThrows(RuntimeException.class, () -> source.getSchema(7));
    Assertions.assertFalse(e instanceof YangSchemaCompiler.YangSchemaException);
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final YangSchemaSource source = new RegistryYangSchemaSource(
        "http://registry:8081/",
        "ietf-interfaces",
        "druid",
        new DefaultPasswordProvider("secret"),
        60_000L
    );
    final YangSchemaSource roundTripped = mapper.readValue(mapper.writeValueAsString(source), YangSchemaSource.class);
    Assertions.assertEquals(source, roundTripped);
    Assertions.assertEquals("http://registry:8081", ((RegistryYangSchemaSource) roundTripped).getUrl());

    final YangSchemaSource directory = mapper.readValue(
        "{\"type\": \"directory\", \"path\": \"/opt/yang\"}",
        YangSchemaSource.class
    );
    Assertions.assertEquals(new DirectoryYangSchemaSource("/opt/yang"), directory);
  }
}
