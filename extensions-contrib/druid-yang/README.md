<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# druid-yang

A Druid extension for ingesting network telemetry modelled in YANG ([RFC 7950](https://datatracker.ietf.org/doc/html/rfc7950)),
such as YANG-Push ([RFC 8641](https://datatracker.ietf.org/doc/html/rfc8641)) notifications that a collector puts on Kafka,
or RESTCONF responses.

It provides one input format, `yangJson`, for YANG instance data encoded as JSON per
[RFC 7951](https://datatracker.ietf.org/doc/html/rfc7951). It is a regular Druid `InputFormat`, usable on its own or
as the `valueFormat` of the `kafka` input format. So far it has been exercised with Kafka streaming ingestion and
unit tests; native batch and MSQ are expected to work but are untested.

Values can be typed from the actual YANG modules, fetched per message from a Confluent-compatible schema registry
by the schema id in the Kafka `schema-id` header. This follows
[draft-ietf-nmop-yang-message-broker-integration](https://datatracker.ietf.org/doc/draft-ietf-nmop-yang-message-broker-integration/)
and works with the [network-analytics/yang-kafka-integration](https://github.com/network-analytics/yang-kafka-integration)
schema registry plugin and serializers. Without a schema, values are typed heuristically.

> Status: prototype on the `yang-ingestion` branch. Not yet part of any Druid release.

## Contents

- [Why a dedicated input format](#why-a-dedicated-input-format)
- [How it works](#how-it-works)
- [Building and installing](#building-and-installing)
- [Quick start (local)](#quick-start-local)
- [Configuration reference](#configuration-reference)
- [YANG schemas](#yang-schemas)
- [Recipe: all models in one datasource](#recipe-all-models-in-one-datasource)
- [Querying](#querying)
- [Development](#development)
- [Limitations and roadmap](#limitations-and-roadmap)

## Why a dedicated input format

YANG-Push JSON is valid JSON, so the stock `json` input format can read it, but the result is hard to use:

| RFC 7951 / YANG-Push trait | With `json` | With `yangJson` |
|---|---|---|
| Member names carry module prefixes (`"ietf-interfaces:interfaces"`, `"ietf-ip:ipv4"`) | Column names like `ietf-ip:ipv4` | Prefixes stripped |
| 64-bit integers and `decimal64` are JSON **strings** (`"in-octets": "841038355987"`) | String columns unless cast one by one | Numbers, per the YANG schema when one is configured |
| The `empty` type is `[null]` | An array containing null | `true` |
| One notification holds a whole list (all interfaces, all BGP neighbours) | One row per message | One row per list entry |
| Data is wrapped in `notification` / `envelope` / `push-update` / `datastore-contents`, and collectors may add a telemetry `message` around that | Every path starts four to six levels deep | Envelope and collector metadata on every row, data at the top |
| Two envelope styles name the timestamp differently (`eventTime` vs `event-time`) | Two timestamp columns | Always `eventTime` |
| On-change updates are YANG-Patch edits, not data trees | Opaque edit list | Rebuilt into rows shaped like periodic ones |
| One topic mixes many subscriptions and models | Can't separate them | Named rules; unmatched messages can be counted as thrown away |

## How it works

For each JSON document (one Kafka message, or one object in a file):

```
  document
     │
     ▼
  resolve schema ─── schema-id header → registry (cached per id), or a local directory, or none
     │
     ▼
  normalize ─── strip module prefixes; without a schema, type values heuristically
     │
     ▼
  unwrap envelope ─── copy eventTime, hostname, sequence-number, subscription-id, ... into an envelope
     │                 telemetry message  → its metadata flattened into the envelope, then its payload
     │                 push-update        → 1 data tree
     │                 push-change-update → 1 data tree per YANG-Patch edit (rebuilt from target + value,
     │                                      list keys restored from the target using the schema)
     ▼
  type by schema ─── uint64 "123" → 123, decimal64 "1.5" → 1.5, empty [null] → true, strings stay strings
     │
     ▼
  apply rules ─── each rule's list entries become rows: envelope + ancestor keys + entry
     │             no rule matched → parse error, or an envelope-only row (onUnmatched: emit)
     ▼
  flattenSpec (optional) → Druid row
```

Example: one `ietf-interfaces` push-update with 12 interfaces and the rule `interfaces/interface` becomes 12 rows,
each with `eventTime`, `hostname`, `subscription-id` and that interface's leaves (`name`, `oper-status`, ...), plus
its containers (`statistics`, `ipv4`, `ipv6`, ...) as nested columns.

## Building and installing

### Against this branch

The module is registered in the root `pom.xml` and in the distribution's pull-deps list, so a normal distribution
build includes it:

```bash
mvn clean install -Pdist -DskipTests
```

To build just this extension and what it depends on:

```bash
mvn -pl extensions-contrib/druid-yang -am install -DskipTests
```

Master requires JDK 25 (`java.version` in the root pom).

The build produces a single shaded jar (about 2 MB) that bundles [yangkit](https://github.com/yang-central/yangkit),
the YANG parser the network-analytics schema registry plugin uses, with its ANTLR runtime relocated to
`org.apache.druid.data.input.yang.shaded.org.antlr`. Relocation is required: yangkit's generated parsers need ANTLR
4.9.3, while Druid ships a different version, and the serialized parser formats are incompatible.

### Into an existing Druid installation

The extension uses only long-stable Druid APIs (`IntermediateRowParsingReader`, `NestedInputFormat`,
`MapInputRowParser`), so the jar can also be loaded into an installation running an older Druid release. Set
`java.version` to the Java version that installation runs on, and copy the jar into its extensions directory:

```bash
cd extensions-contrib/druid-yang
mvn clean package -DskipTests -Djava.version=21
mkdir -p $DRUID_HOME/extensions/druid-yang
cp target/druid-yang-*-SNAPSHOT.jar $DRUID_HOME/extensions/druid-yang/druid-yang.jar
```

Use `clean`: without it, classes already compiled for a newer Java can end up in the jar, and services fail at
startup with `UnsupportedClassVersionError`.

### Enabling it

Add it to the load list on **every** service, the overlord included:

```properties
druid.extensions.loadList=[..., "druid-kafka-indexing-service", "druid-yang"]
```

Several services deserialize and re-serialize ingestion specs with their own copy of the extension, and Druid
ignores unknown JSON properties. A service running an older version therefore **silently drops** newer options:

- the overlord drops them from the stored supervisor spec (seen with `rules`);
- a middleManager or indexer drops them from the task spec it hands to the peon (seen with `schema`, which made
  a task quietly fall back to heuristic typing).

After upgrading the jar, restart the overlord **and** every middleManager/indexer, then check the effective spec in
a running task's log. Peons start fresh JVMs and load the new jar on their own. When replacing the jar on disk,
remove the old file and copy the new one rather than overwriting it in place, since running JVMs still read it.

## Quick start (local)

Assumes a local Druid quickstart with the extension loaded, and a Kafka broker on `localhost:9092`.

**1. Create a topic and start the generator.** `dev/yang_push_generator.py` has no dependencies beyond Python 3.
It simulates 10 routers with four subscriptions each:

| Subscription | Model | Kind | Every |
|---|---|---|---|
| 1000+n | `ietf-interfaces` with `ietf-ip` ipv4/ipv6 and a vendor optics augmentation | `push-update` | 60 s |
| 2000+n | `openconfig-network-instance` BGP neighbours | `push-update` | 30 s |
| 3000+n | `ietf-interfaces` oper-status | `push-change-update` | on change |
| 4000+n | `ietf-interfaces` subtree filter: name + statistics | `push-update` | 10 s |

Two of the routers use the RFC 8641 envelope, which has no hostname, and the rest use the notif-envelope draft.
`--key` makes the hostname the Kafka key, and `--backfill 1` sends an hour of history first.

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic yang-telemetry --partitions 3

python3 extensions-contrib/druid-yang/dev/yang_push_generator.py --key --backfill 1 \
  | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic yang-telemetry \
      --property parse.key=true --property key.separator=$'\t'
```

**2. Submit the supervisor.** One supervisor reads the topic once and writes every model into one datasource:

```bash
curl -XPOST -H 'Content-Type: application/json' localhost:8888/druid/indexer/v1/supervisor \
  -d @extensions-contrib/druid-yang/dev/kafka-supervisor-yang-telemetry.json
```

The spec uses the schema registry from step 3 for subscriptions that have a `schema-id`, and heuristic typing for
the rest. Without a registry, remove its `schema` and `onMissingSchemaId`.

**3. Optional: with YANG schemas from a schema registry.** `dev/schema-registry/` runs Confluent Schema Registry
8.0 with the network-analytics YANG plugin (in Docker, on `localhost:18081`, with its own Kafka as storage).
`dev/register_yang_schemas.py` registers a directory of modules the way the draft describes, and the generator's
producer mode sends the `schema-id` header as the network-analytics serializers do (needs `pip install kafka-python`).
Run this generator **instead of** the one in step 1:

```bash
(cd extensions-contrib/druid-yang/dev/schema-registry && ./fetch-plugin.sh && docker compose up -d)

python3 extensions-contrib/druid-yang/dev/register_yang_schemas.py --url http://localhost:18081 \
  --modules extensions-contrib/druid-yang/src/test/resources/yang/modules \
  --subscription interfaces-push=ietf-interfaces --augments ietf-ip,acme-if-ext
# ... prints schema-id=6

python3 extensions-contrib/druid-yang/dev/yang_push_generator.py --backfill 1 \
  --bootstrap localhost:9092 --topic yang-telemetry \
  --schema-id interfaces-full=6 --schema-id stats-subtree=6 --schema-id on-change=6
```

Only the interface subscriptions get a schema id here; BGP messages have none (the OpenConfig modules aren't
registered), which is what `onMissingSchemaId: heuristic` in the supervisor spec is for.

In producer mode the Kafka key is `<hostname>\n<subscription id>\n<xpath>` as in the draft; the supervisor's
`regex` key format reads the key line by line, so `kafka.key` is the hostname.

**4. Query.**

```sql
-- Inbound throughput per interface, from the cumulative counters
WITH d AS (
  SELECT device, name,
         in_octets - LAG(in_octets) OVER (PARTITION BY device, name ORDER BY __time) AS d_in,
         TIMESTAMPDIFF(SECOND, LAG(__time) OVER (PARTITION BY device, name ORDER BY __time), __time) AS dt
  FROM yang_telemetry
  WHERE "yang-rule" = 'interfaces' AND "yang-update" = 'push-update'
    AND __time > CURRENT_TIMESTAMP - INTERVAL '15' MINUTE
)
SELECT device, name, ROUND(AVG(d_in * 8.0 / dt) / 1e6, 1) AS avg_in_mbps
FROM d WHERE dt > 0 AND d_in >= 0
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
```

**Starting over.** To reset the local setup, terminate the supervisor, drop the datasource and recreate the topic:

```bash
curl -XPOST localhost:8888/druid/indexer/v1/supervisor/yang_telemetry/terminate
curl -XDELETE localhost:8888/druid/coordinator/v1/datasources/yang_telemetry   # marks segments unused
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic yang-telemetry
(cd extensions-contrib/druid-yang/dev/schema-registry && docker compose down -v)  # if you used the registry
```

Wait for the supervisor's tasks to finish before dropping the datasource, or they may publish segments afterwards.
A terminated supervisor's stored offsets stay with the datasource name; resubmitting for the same datasource with
a recreated topic may need a supervisor reset (`POST .../supervisor/yang_telemetry/reset`).

## Configuration reference

### `yangJson` properties

| Property | Default | Description |
|---|---|---|
| `rules` | none | List of `{"name": ..., "path": ...}`. For each rule, every entry of the YANG list at `path` becomes one row, tagged with the rule's `name` (default: the path) in `yang-rule`. A message can match several rules. See [Rules](#rules). |
| `explodePath` | none | Shorthand for a single rule named after its path. Mutually exclusive with `rules`. |
| `onUnmatched` | `fail` | What to do with a message that matches no rule. `fail` reports a parse exception, counted as `unparseable`. `emit` produces one row holding only the envelope fields and no `yang-rule`, so that a `transformSpec` filter can count it as `thrownAway`. With no rules, every message matches. |
| `stripModulePrefixes` | `true` | `"ietf-interfaces:interfaces"` becomes `"interfaces"`. If two modules use the same local name in one object, the second keeps its qualified name. Values such as identityrefs (`"iana-if-type:ethernetCsmacd"`) are left untouched. |
| `coerceTypes` | `true` | Heuristic typing, used when there is no schema and for nodes the schema doesn't cover: reverses the RFC 7951 string encoding of `int64`/`uint64`/`decimal64` (`"123"` becomes `123`; values above `Long.MAX_VALUE` become doubles) and turns `[null]` into `true`. Any string that looks like a plain integer or decimal without leading zeros is converted, including string leaves such as serial numbers; configure a `schema` to avoid that. |
| `unwrapEnvelope` | `true` | Unwraps the collector's telemetry `message` and `payload`, then `notification` / `envelope` / `contents` / `notification-contents` / `push-update` / `push-change-update` / `datastore-contents`, copying their scalar members to every row. See [Added columns](#added-columns) and [Telemetry messages](#telemetry-messages). |
| `schema` | none | Where to get the YANG schema: `{"type": "registry", ...}` or `{"type": "directory", "path": ...}`. See [YANG schemas](#yang-schemas). |
| `schemaIdHeader` | `schema-id` | Kafka header holding the message's schema id. |
| `schemaIdEncoding` | `int32` | `int32`: 4-byte big-endian integer, as written by the network-analytics serializers. `string`: decimal digits, as shown in the draft's examples. |
| `onMissingSchemaId` | `fail` | For messages without a schema id when the `schema` needs one (a registry without `subject`): `fail` makes them parse exceptions; `heuristic` processes them without a schema. Use `heuristic` while only some subscriptions have registered schemas. |
| `validation` | `none` | `strict` validates each message against its schema and makes an invalid one a parse exception listing the first problems; requires a `schema`. See [Validation](#validation). |
| `flattenSpec` | none | Standard Druid JSONPath flattenSpec, applied to each row *after* the steps above. |
| `keepNullColumns` | `false` | Same as for the `json` input format. |

### Rules

A rule's `path` is a `/`-separated list of node names from the top of the data tree to a list, e.g.
`interfaces/interface` or
`network-instances/network-instance/protocols/protocol/bgp/neighbors/neighbor`. Segments match with or without
module prefixes. If the path ends at a container instead of a list, the container is one row.

Lists crossed on the way are fanned out as well. The scalar leaves of every list entry and container on the path
(list keys, a notification's identifying leaves) are copied onto each row as `<node>.<leaf>`, e.g.
`network-instance.name`, `protocol.identifier`, `incident-notification.incident-no`. Nested containers of those
ancestors are not copied.

### Added columns

| Column | Content |
|---|---|
| `eventTime` | The notification time, from `eventTime` (RFC 8040/8641) or `event-time` (notif-envelope draft). In a telemetry message without either, the node's export time, else the collection time. Use it in `timestampSpec`. |
| `subscription-id` | The YANG-Push subscription `id`. |
| `yang-update` | `push-update` or `push-change-update`. |
| `yang-rule` | Name of the rule that produced the row. Absent on `onUnmatched: emit` rows. |
| `yang-operation`, `yang-target` | For `push-change-update` only: the YANG-Patch edit's `operation` and `target`. |

Other scalar envelope fields (`hostname`, `sequence-number`, `patch-id`, ...) keep their names. If the row's data has
a field with the same name and a different value, the data wins and the envelope's value is kept as
`envelope.<name>`. For example, an incident that embeds another device's notification has that device's `hostname`
plus `envelope.hostname`, the system that published the incident.

### Telemetry messages

YANG-Push receivers that put notifications on a message broker, such as NetGauze and pmacct, can wrap each one in
the `ietf-telemetry-message:message` structure of
[draft-ietf-nmop-message-broker-telemetry-message](https://datatracker.ietf.org/doc/draft-ietf-nmop-message-broker-telemetry-message/).
The notification is its `payload` (a bare `push-update`, or a notif-envelope around one), and the rest is metadata
about the device, the collector and the subscription. A `message` is treated as a telemetry message only if it
has both `payload` and `telemetry-message-metadata`, so data nodes that happen to be called `message` are left alone.

The metadata goes on every row, each leaf named after its immediate container:

| Column | From |
|---|---|
| `network-node-manifest.name`, `.vendor`, `.software-version`, ... | the exporting device |
| `data-collection-manifest.name`, `.vendor`, ... | the collector |
| `telemetry-message-metadata.node-export-timestamp`, `.collection-timestamp` | when the device exported the message and when the collector received it; their difference is the export delay |
| `telemetry-message-metadata.sequence-number`, `.export-address`, `.export-port`, `.collection-address`, `.collection-port`, `.session-protocol`, `.notification-event` | the collection session |
| `yang-push-subscription.id`, `.xpath-filter`, `.datastore`, `.transport`, `.encoding`, ... | the subscription (`ietf-yang-push-telemetry-message`) |
| `periodic.period`, `on-change.dampening-period`, ... | the subscription's update trigger |
| `yang-push-subscription.module` | the subscribed modules, as an array |
| `labels.<name>` | each network-operator label, with its number, string or anydata value |

A bare `push-update` payload has no notification time and no hostname. `eventTime` then falls back to the export
timestamp, and the device is in `network-node-manifest.name`, so derive a device column with
`nvl("hostname", nvl("network-node-manifest.name", "kafka.key"))`.

### On-change updates (`push-change-update`)

Each YANG-Patch edit is turned back into a data tree from its `target` (e.g.
`/ietf-interfaces:interfaces/interface=eth0`) and `value`, and the rules are applied to that tree. An on-change
edit of an interface therefore produces a row shaped like the periodic rows, typically with far fewer columns.

With a schema, list keys in the target are turned into leaves: an edit targeting
`.../interface=eth0/statistics` produces a row with `name` = `eth0`, typed like any other leaf (a `uint8` key becomes a
number). Without a schema the key names are unknown, the row has no `name`, and `yang-target` is the only place the
key appears.

A `delete` or `remove` edit has no value. It produces a row with just the envelope and edit columns if its target
is on, below or above a rule's path.

## YANG schemas

With a `schema`, each leaf is typed by its YANG built-in type, following typedefs (`yang:counter64` is a `uint64`):

| YANG type | RFC 7951 encoding | Ingested as |
|---|---|---|
| `int64`; `uint64` counters and gauges (`counter64`, `gauge64`, …) | string | long (double above `Long.MAX_VALUE`) |
| other `uint64` (identifiers, serial numbers) | string | long, or the exact string above `Long.MAX_VALUE` |
| other integer types | number | long |
| `decimal64` | string | double |
| `boolean` | `true`/`false` | boolean |
| `empty` | `[null]` | `true` |
| union | per member | typed only if the first member is numeric; otherwise as encoded |
| `leafref` | as its target | typed as the leaf it points to |
| everything else (`string`, `enumeration`, `identityref`, `bits`, `binary`, ...) | string | string, even if it looks like a number |

Nodes the schema doesn't know (a module that wasn't registered, an unexpected augment, `anydata` content) fall back
to the `coerceTypes` heuristic, or stay as encoded if it's off. Augments, `uses`/groupings and typedefs are resolved
by the parser, and `choice`/`case` are transparent, as in the JSON encoding. The data tree is matched by local name,
or by `module:name` where two modules collide.

Besides datastore data, the schema covers **notifications**, including YANG 1.1 notifications nested in containers
and lists, as top-level nodes named after the notification (e.g. `incident-notification`). RPCs and actions aren't
instance data and are left out. `sx:structure` (RFC 8791) definitions, such as the telemetry `message` and the
notification `envelope`, are only used for [validation](#validation).

A device's YANG library often contains a few modules that yangkit rejects, typically ones unrelated to the
subscription (a broken augment in some Broadband Forum module, say). Compilation is therefore tolerant: modules that
fail to parse or validate, and modules importing them, are logged as broken and left out, and the rest of the schema
is used. Only a schema in which every module is broken is rejected. With `validation: strict`, a message using a node
of a broken module is reported as invalid, with the module's error.

### Validation

With `"validation": "strict"`, each message is checked against its schema before it is turned into rows, and an
invalid message is a parse exception (counted as `unparseable`, and logged with `logParseExceptions`) such as:

```
Message is not valid against its YANG schema, 1 problem(s):
/ietf-telemetry-message:message/telemetry-message-metadata: missing mandatory leaf [notification-event]
```

What is checked:

- Every member is a known node of its parent, of the right kind (container, list, leaf, leaf-list, anydata).
  Top-level members may be data nodes, notifications or structures, so a telemetry `message`, a bare notification
  `envelope` or a plain notification all validate.
- The contents of `anydata` nodes (the telemetry `payload`, an envelope's `contents`, a push-update's
  `datastore-contents`) are validated from the top of the schema, as in
  [draft-aelhassany-using-yang-library-to-validate-anydata](https://datatracker.ietf.org/doc/draft-aelhassany-using-yang-library-to-validate-anydata/).
- Values match their types, with yangkit evaluating ranges, lengths, patterns, enumerations and bits. Identityrefs
  must name a known identity derived from the type's base. Unions accept a value any member accepts. Integers are
  accepted as JSON numbers or strings, whatever their size.
- List entries have all their keys.
- Leaves that are unconditionally mandatory (no `when`, `if-feature`, enclosing `choice`, or augment) are present in
  structures and notifications. They are not enforced in datastore data, which subscription filters legitimately
  leave partial.

Not checked: `must` and `when` expressions, leafref targets, `unique`, `min-elements`/`max-elements`, and the
`value` of YANG-Patch edits (it is relative to the edit's target). RFC 7952 metadata members (`@...`) are ignored.

### From a schema registry

```json
"schema": {
  "type": "registry",
  "url": "http://schema-registry:8081",
  "subject": "interfaces-push",
  "username": "druid",
  "password": { "type": "environment", "variable": "SCHEMA_REGISTRY_PASSWORD" }
}
```

| Property | Default | Description |
|---|---|---|
| `url` | required | Registry base URL. |
| `subject` | none | Used for messages **without** a schema id header: the latest version of this subject. Without it, such messages are parse exceptions. |
| `username`, `password` | none | HTTP basic auth; `password` is a Druid [password provider](https://druid.apache.org/docs/latest/operations/password-provider). |
| `latestRefreshMillis` | 300000 | How long the latest version of `subject` is cached. |

For each schema id, the source fetches `GET /schemas/ids/{id}` and then every reference transitively
(`GET /subjects/{subject}/versions/{version}`), compiles the modules, and caches the result for the life of the
task. In the draft's layout the subscribed module lists the modules that augment it as references too, so the same
module can be reached more than once; each module is used once. Schemas must have type `YANG`.

Errors are split so that data is never silently lost:

- A problem with the message itself (no id and no `subject`, a malformed header, an id the registry doesn't have,
  a non-YANG or invalid schema) is a parse exception for that message.
- Failing to reach the registry is not: the task fails and is retried from its last committed offset.

The schema id is read from the Kafka record's headers, which works both when `yangJson` is the supervisor's input
format and when it is the `valueFormat` of the `kafka` input format. There are no headers in batch ingestion; use
`subject` or a directory there.

### From a directory

```json
"schema": { "type": "directory", "path": "/opt/yang/modules" }
```

All `.yang` files in the directory are compiled once and used for every message, whatever its schema id. Every
import must be present. The directory must exist on the services that run ingestion tasks.

## Notifications (e.g. network incidents)

Some models deliver their data as YANG **notifications** rather than as datastore updates. One example is
`ietf-incident` from [draft-ietf-nmop-network-incident-yang](https://datatracker.ietf.org/doc/draft-ietf-nmop-network-incident-yang/),
where an incident is raised, updated and cleared through `incident-notification`. In the notification envelope the
notification sits under `notification-contents`, and a rule names it by its node name:

```json
"rules": [
  { "name": "incidents",       "path": "incident-notification" },
  { "name": "incident-causes", "path": "incident-notification/probable-causes/probable-cause" },
  { "name": "incident-events", "path": "incident-notification/events/event" }
]
```

- **Incident rows** carry every leaf of the incident, typed by the schema. `incident-no` is a `uint64` serial
  number and stays exact; `incident-qualifier` is a string even when it looks numeric.
- **Cause and event rows** carry the incident's leaves as `incident-notification.<leaf>`
  (`incident-notification.incident-no`, `.priority`, `.status`), so they can be joined or filtered by incident.
- Each event type is a container inside the event (`alarm`, `metric` or `notification`). An event of type
  `notification` embeds another device's notification, with its own `event-time`, `hostname` and `sequence-number`,
  and an `anydata` `contents`. A rule down to `incident-notification/events/event/notification` makes those the
  row's fields and keeps the incident publisher's as `envelope.hostname` and so on.

Modelling notes:

- **Incidents have a lifecycle.** The same `incident-no` arrives as `raised`, then `updated` (any number of times),
  then `cleared`, with `ack-status` alongside. Ingest every notification and query the current state with
  `LATEST_BY("status", __time)` … `GROUP BY "incident-no"`; time to resolve comes from `occur-time` and
  `clear-time` (RFC 3339 strings; use `TIME_PARSE`).
- **They are a good reason for a separate datasource.** Incidents are low volume and kept for a long time, unlike
  telemetry counters (see [When to split into several datasources](#when-to-split-into-several-datasources)).
- **They reference other models.** Alarm events point at `ietf-alarms` alarms (resource, alarm type, qualifier);
  sources and causes point at `ietf-network` nodes. Ingest `ietf-alarms`' `alarm-notification` too, and provide
  topology attributes as a Druid lookup keyed by node id.

## Recipe: all models in one datasource

Collectors usually put every subscription of every device on one topic. One supervisor with one rule per model
reads the topic once and writes all rows into one datasource; `yang-rule` and `yang-update` tell them apart, and
columns of other models are simply null (cheap in Druid, especially with schema discovery):

```json
"dataSchema": {
  "dataSource": "yang_telemetry",
  "timestampSpec": { "column": "eventTime", "format": "iso" },
  "dimensionsSpec": { "useSchemaDiscovery": true, "dimensions": ["device", "yang-rule", "yang-update", "..."] },
  "transformSpec": {
    "transforms": [
      { "type": "expression", "name": "device", "expression": "nvl(\"hostname\", \"kafka.key\")" }
    ]
  }
},
"ioConfig": {
  "inputFormat": {
    "type": "kafka",
    "keyFormat": { "type": "regex", "pattern": "(.*)", "columns": ["hostname"] },
    "valueFormat": {
      "type": "yangJson",
      "rules": [
        { "name": "interfaces", "path": "interfaces/interface" },
        { "name": "bgp-neighbors", "path": "network-instances/network-instance/protocols/protocol/bgp/neighbors/neighbor" }
      ],
      "schema": { "type": "registry", "url": "http://schema-registry:8081" }
    }
  }
}
```

- The RFC 8641 envelope carries no device identity, which is why `device` falls back to the Kafka key. The `regex`
  key format reads the key line by line and Druid keeps the first line, so with the draft's
  `<hostname>\n<subscription id>\n<xpath>` key, `kafka.key` is the hostname.
- Keep `onUnmatched` at `fail` here: with a rule for every model on the topic, a message that matches none is
  unexpected and should show up as unparseable.
- Queries should always filter on `yang-rule` (and usually `yang-update`). Compacting with range partitioning on
  `yang-rule`, `device` lets those filters prune segments.
- A flattenSpec can pull out fields of several models; paths that don't exist in a row are null.

The complete spec is `dev/kafka-supervisor-yang-telemetry.json`.

### When to split into several datasources

Split only when models need different treatment that Druid configures per datasource: retention (e.g. 10-second
counters for a week, BGP state for a year), rollup or compaction, or access control. Then run one supervisor per
datasource on the same topic, each with only its own rule, `onUnmatched: emit`, and a filter that keeps its rows:

```json
"filter": {
  "type": "and",
  "fields": [
    { "type": "not", "field": { "type": "null", "column": "yang-rule" } },
    { "type": "equals", "column": "yang-update", "matchValueType": "STRING", "matchValue": "push-update" }
  ]
}
```

Each supervisor reads the whole topic and discards what isn't its own; discarded rows show up as `thrownAway`
(reason `null`, which is how Druid reports rows rejected by a `transformSpec` filter). Examples:
`dev/kafka-supervisor-yang-interfaces.json`, `dev/kafka-supervisor-yang-interface-events.json` and
`dev/kafka-supervisor-yang-bgp-neighbors.json`. Several supervisors can also write into one datasource
(`useConcurrentLocks: true` in their context), for example one per topic.

## Querying

Nested containers such as `statistics` become nested (`COMPLEX<json>`) columns when using schema discovery:

```sql
SELECT __time, device, name, JSON_VALUE(statistics, '$[''in-octets'']' RETURNING BIGINT) AS in_octets
FROM yang_telemetry
WHERE "yang-rule" = 'interfaces'
```

YANG names often contain `-`, so quote them in SQL (`"oper-status"`) and use bracket notation in JSON paths
(`$['in-octets']`).

Nested lists can be unnested at query time:

```sql
SELECT device, name, JSON_VALUE(a, '$.ip') AS ip
FROM yang_telemetry CROSS JOIN UNNEST(JSON_QUERY_ARRAY(ipv6, '$.address')) AS u(a)
WHERE "yang-rule" = 'interfaces'
```

For the console's **Explore** view, `dev/explore-interface-traffic.sql` is a source query that turns the cumulative
counters into per-interface, per-minute rates and defines measures (total and per-interface Gbps, peak utilisation,
errors) with `--:MEASURE` comments. It avoids window functions: Explore applies its filters outside the source
query, so a `LAG` over the whole table would hit the subquery row limit. The query bounds its own scan to the last
day for the same reason.

Interface outages from the on-change stream:

```sql
SELECT device, name, COUNT(*) FILTER (WHERE "oper-status" = 'down') AS outages
FROM yang_telemetry
WHERE "yang-rule" = 'interfaces' AND "yang-update" = 'push-change-update'
GROUP BY 1, 2 ORDER BY 3 DESC
```

## Development

### Layout

```
src/main/java/org/apache/druid/data/input/yang/
  YangModule.java            registers the input format
  YangJsonInputFormat.java   configuration (type "yangJson")
  YangJsonNodeReader.java    reads JSON documents; one message → many rows
  YangJsonTransformer.java   normalization, envelope unwrapping, YANG-Patch, rule matching
  YangExplodeRule.java       a named explode path
  schema/
    YangSchemaSource.java           where schemas come from (registry, directory)
    RegistryYangSchemaSource.java   Confluent REST client, reference resolution, cache
    YangSchemaCompiler.java         yangkit → YangSchemaIndex (tolerates broken modules)
    YangValueChecks.java            per-type value checks built on yangkit restrictions
    YangSchemaIndex.java            data tree, structures, list keys, value types and checks; no parser types
    YangValidator.java              validates RFC 7951 instance data against a YangSchemaIndex
    RecordHeaders.java              reads Kafka headers without depending on the Kafka extension
src/test/resources/yang/     YANG-Push fixtures (ietf-interfaces, notif-envelope, OpenConfig BGP, on-change, and the
                             telemetry message examples of the draft)
src/test/resources/yang/modules/   ietf-interfaces, ietf-ip, ietf-yang-types, ietf-inet-types, and a fictitious
                                   acme-if-ext optics augment
src/test/resources/yang/telemetry-modules/   the IETF modules of a telemetry message carrying YANG-Push, for
                                             validation tests
dev/                         generator, Kafka supervisor specs, schema registry setup and registration script
```

`YangJsonTransformer` works purely on Jackson trees, so most behaviour is unit-tested there without Druid.
`YangJsonInputFormatTest` covers serde and reading through Druid's reader stack. `YangSchemaReaderTest` runs the
reader against `FakeSchemaRegistry`, an in-process HTTP server that serves schemas the way the network-analytics
plugin stores them, with a fake Kafka record carrying the `schema-id` header.

### Running the tests

```bash
mvn -pl extensions-contrib/druid-yang test
```

This needs JDK 25. On older JDKs the forked test JVM fails at startup because of `-XX:+UseCompactObjectHeaders` in
the root pom's surefire `argLine`, and Maven then waits on the fork indefinitely instead of failing. To run the
tests on JDK 24, compile with `-Djava.version=24` and run JUnit directly:

```bash
mvn -pl extensions-contrib/druid-yang -o test-compile dependency:build-classpath \
  -Djava.version=24 -Denforcer.skip -Dmdep.outputFile=/tmp/cp.txt
P=org.apache.druid.data.input.yang
java -cp "extensions-contrib/druid-yang/target/test-classes:extensions-contrib/druid-yang/target/classes:$(cat /tmp/cp.txt)" \
  org.junit.runner.JUnitCore $P.YangJsonTransformerTest $P.YangJsonInputFormatTest $P.YangSchemaTypingTest \
  $P.YangSchemaReaderTest $P.YangIncidentTest $P.schema.YangSchemaCompilerTest $P.schema.RegistryYangSchemaSourceTest \
  $P.schema.YangValidatorTest
```

### A contract to keep in mind

`IntermediateRowParsingReader` requires `parseInputRows` to return **at least one row** per message. An empty list
makes the task fail with `NoSuchElementException`, and the supervisor then restarts it from the same offset.
That's why unmatched messages either raise a `ParseException` or become an envelope-only row, and are never
silently skipped. A `null` row would be counted as thrown away by streaming ingestion, but `KafkaInputReader` fails
on it with a `NullPointerException` when it adds the key and header columns.

## Limitations and roadmap

- **Schema coverage.** Validation is this extension's own, built on yangkit's schema model and type restrictions;
  see [Validation](#validation) for what it leaves out. Unions are only typed when their first member is numeric.
  YANG features from the registry's schema metadata are not applied yet: every feature is treated as enabled.
  yangkit 1.4.5 from Maven Central is used; the network-analytics plugin runs a 1.5.0 fork whose changes are in data
  validation, which this extension doesn't use.
- **Schema registration layout.** The subject-per-module layout with augments as references follows the draft and
  the plugin's tests. It hasn't yet been checked against what YANG-Push receivers such as NetGauze register.
- **Generating specs from schemas.** The draft expects ingestion specs to be derived from the schema; today rules
  and dimensions are still written by hand.
- **JSON only.** XML (NETCONF), CBOR ([RFC 9254](https://datatracker.ietf.org/doc/html/rfc9254)) and gNMI/MDT
  protobuf aren't supported.
- **Rows mirror subscriptions.** A subtree-filtered subscription produces sparse rows next to full ones in the same
  datasource. Separate them with a filter on `subscription-id` if that matters for your queries.
- **`uint64` above `Long.MAX_VALUE`** becomes a double and loses precision.
- **YANG-Patch operations are not interpreted.** Edits with a value (`create`, `merge`, `replace`, `insert`) become
  data rows; edits without one (`delete`, `remove`, `move`) become envelope-and-edit-only rows. The operation is in
  `yang-operation` for queries to act on.
