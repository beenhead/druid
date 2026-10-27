#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generates a mixed stream of YANG-Push (RFC 8641) notifications, JSON-encoded per RFC 7951, one message
per line, as a telemetry collector would put them on a single Kafka topic.

Every simulated router has four subscriptions:

  1000+n  ietf-interfaces, full subtree        periodic push-update, every 6 ticks
          (with ietf-ip ipv4/ipv6 and a fictitious acme-if-ext optics augmentation)
  2000+n  openconfig-network-instance BGP      periodic push-update, every 3 ticks
  3000+n  ietf-interfaces oper-status           on-change push-change-update (YANG-Patch)
  4000+n  ietf-interfaces subtree filter        periodic push-update, every tick
          selecting only interface name + statistics

Most routers wrap notifications in the draft-ietf-netconf-notif-envelope envelope (which carries
hostname); the last --legacy-routers use the plain RFC 8040 / RFC 8641 notification, which does not.

Kinds: interfaces-full, bgp, on-change, stats-subtree (see --kinds).

A per-kind message count is written to stderr at the end of the backfill and on exit.

With --bootstrap and --topic, messages are produced directly to Kafka (needs the kafka-python package) the way
draft-ietf-nmop-yang-message-broker-integration describes: key "<hostname>\\n<subscription id>\\n<xpath>", headers
"content-type: application/yang-data+json" and, for kinds given with --schema-id, "schema-id" as a 4-byte
big-endian integer (as the network-analytics YANG serializers write it).

With --key, each line is "<hostname><TAB><json>", for kafka-console-producer's parse.key, so that every message
(including those from legacy routers) carries the device as its Kafka key, as collectors usually do.

Pipe into Kafka with:
  python3 yang_push_generator.py | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic yang-telemetry

or, keyed by device:
  python3 yang_push_generator.py --key | kafka-console-producer.sh --bootstrap-server localhost:9092 \\
      --topic yang-telemetry --property parse.key=true --property key.separator=$'\\t'
"""

import argparse
import collections
import json
import random
import struct
import sys
import time
from datetime import datetime, timezone
from urllib.parse import quote

FULL_EVERY = 6
BGP_EVERY = 3
KINDS = ["interfaces-full", "bgp", "on-change", "stats-subtree"]
XPATHS = {
    "interfaces-full": "/ietf-interfaces:interfaces",
    "bgp": "/openconfig-network-instance:network-instances",
    "on-change": "/ietf-interfaces:interfaces/interface/oper-status",
    "stats-subtree": "/ietf-interfaces:interfaces/interface/statistics",
}


def iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Interface:
    def __init__(self, rng, index, boot_time):
        self.rng = rng
        self.uplink = index >= 8
        self.name = f"HundredGigE0/1/{index - 8}" if self.uplink else f"GigabitEthernet0/0/{index}"
        self.speed = 100_000_000_000 if self.uplink else 1_000_000_000
        self.enabled = rng.random() > 0.05
        self.up = self.enabled
        self.utilisation = rng.uniform(0.01, 0.6)
        self.in_octets = rng.randrange(10 ** 9, 10 ** 12)
        self.out_octets = rng.randrange(10 ** 9, 10 ** 12)
        self.in_pkts = self.in_octets // 700
        self.out_pkts = self.out_octets // 700
        self.in_errors = 0
        self.out_discards = 0
        self.discontinuity_time = iso(boot_time)
        self.ipv4 = [f"10.{rng.randrange(256)}.{rng.randrange(256)}.1"] if rng.random() > 0.3 else []
        # dual-stack on uplinks and some access ports; a few ports are IPv6-only
        self.ipv6 = []
        if self.uplink or rng.random() < 0.4:
            self.ipv6 = [f"2001:db8:{rng.randrange(0x10000):x}:{index:x}::1"]
            if rng.random() < 0.3:
                self.ipv6.append(f"2001:db8:{rng.randrange(0x10000):x}:{index:x}::2")
            if rng.random() < 0.3:
                self.ipv4 = []
        self.ipv6_neighbors = [f"fe80::{rng.randrange(0x10000):x}" for _ in range(rng.randrange(0, 3))] if self.ipv6 else []
        self.optics = self.uplink or rng.random() < 0.25
        self.rx_power = rng.uniform(-6.0, -1.0)
        self.lag = "Port-channel1" if self.uplink and index < 10 else None
        # a string in acme-if-ext, but digits only: typed wrongly by any schema-less heuristic
        self.serial = "%08d" % rng.randrange(10 ** 7, 10 ** 8) if self.optics else None

    def advance(self, seconds):
        """Returns True if oper-status changed."""
        changed = False
        if self.enabled:
            if self.up and self.rng.random() < 0.002:
                self.up, changed = False, True
            elif not self.up and self.rng.random() < 0.3:
                self.up, changed = True, True
        if self.up:
            util = max(0.0, min(1.0, self.rng.gauss(self.utilisation, self.utilisation * 0.2)))
            in_bytes = int(self.speed / 8 * seconds * util)
            out_bytes = int(self.speed / 8 * seconds * util * self.rng.uniform(0.5, 1.2))
            self.in_octets += in_bytes
            self.out_octets += out_bytes
            self.in_pkts += in_bytes // self.rng.randrange(300, 1400)
            self.out_pkts += out_bytes // self.rng.randrange(300, 1400)
            if self.rng.random() < 0.02:
                self.in_errors += self.rng.randrange(1, 500)
            if util > 0.9:
                self.out_discards += self.rng.randrange(1, 100)
        self.rx_power = max(-12.0, min(0.0, self.rx_power + self.rng.gauss(0, 0.05)))
        return changed

    def statistics(self):
        # counter64 in RFC 8343; RFC 7951 encodes 64-bit integers as strings
        return {
            "discontinuity-time": self.discontinuity_time,
            "in-octets": str(self.in_octets),
            "in-unicast-pkts": str(self.in_pkts),
            "in-errors": str(self.in_errors),
            "out-octets": str(self.out_octets),
            "out-unicast-pkts": str(self.out_pkts),
            "out-discards": str(self.out_discards),
        }

    def full(self):
        node = {
            "name": self.name,
            "type": "iana-if-type:ethernetCsmacd",
            "enabled": self.enabled,
            "admin-status": "up" if self.enabled else "down",
            "oper-status": "up" if self.up else "down",
            "speed": str(self.speed),
            "statistics": self.statistics(),
        }
        if self.lag:
            node["higher-layer-if"] = [self.lag]  # leaf-list
        if self.ipv4:
            node["ietf-ip:ipv4"] = {
                "mtu": 1500,
                "address": [{"ip": ip, "prefix-length": 24, "origin": "static"} for ip in self.ipv4],
            }
        if self.ipv6:
            node["ietf-ip:ipv6"] = {
                "mtu": 1500,
                "address": [
                    {"ip": ip, "prefix-length": 64, "origin": "static", "status": "preferred"} for ip in self.ipv6
                ],
                "neighbor": [
                    {"ip": ip, "link-layer-address": "00:00:5e:00:53:%02x" % i, "origin": "dynamic",
                     "is-router": [None], "state": "reachable"}
                    for i, ip in enumerate(self.ipv6_neighbors)
                ],
            }
        if self.optics:
            # decimal64 leaves are strings in RFC 7951
            node["acme-if-ext:optics"] = {
                "serial-number": self.serial,
                "temperature": "%.1f" % self.rng.uniform(30, 55),
                "rx-power": "%.2f" % self.rx_power,
                "tx-power": "%.2f" % self.rng.uniform(-2.0, 0.0),
                "lane": [{"index": i, "rx-power": "%.2f" % (self.rx_power + self.rng.uniform(-0.3, 0.3))}
                         for i in range(1, 5)] if self.uplink else [],
            }
        return node


class BgpNeighbor:
    def __init__(self, rng, address, peer_as):
        self.rng = rng
        self.address = address
        self.peer_as = peer_as
        self.established = True
        self.transitions = rng.randrange(1, 5)
        self.updates_in = rng.randrange(10 ** 4, 10 ** 7)
        self.updates_out = rng.randrange(10 ** 3, 10 ** 5)
        self.prefixes = rng.randrange(10, 900_000)

    def advance(self):
        if self.established and self.rng.random() < 0.003:
            self.established = False
            self.transitions += 1
        elif not self.established and self.rng.random() < 0.2:
            self.established = True
            self.transitions += 1
        if self.established:
            self.updates_in += self.rng.randrange(0, 500)
            self.updates_out += self.rng.randrange(0, 20)

    def to_oc(self):
        v6 = ":" in self.address
        return {
            "neighbor-address": self.address,
            "config": {"neighbor-address": self.address, "peer-as": self.peer_as},
            "state": {
                "neighbor-address": self.address,
                "peer-as": self.peer_as,
                "session-state": "ESTABLISHED" if self.established else "ACTIVE",
                "established-transitions": str(self.transitions),
                "messages": {
                    "received": {"UPDATE": str(self.updates_in), "NOTIFICATION": "0"},
                    "sent": {"UPDATE": str(self.updates_out), "NOTIFICATION": "0"},
                },
            },
            "afi-safis": {"afi-safi": [{
                "afi-safi-name": "openconfig-bgp-types:IPV6_UNICAST" if v6 else "openconfig-bgp-types:IPV4_UNICAST",
                "state": {"prefixes": {
                    "received": self.prefixes if self.established else 0,
                    "installed": int(self.prefixes * 0.9) if self.established else 0,
                    "sent": 10,
                }},
            }]},
        }


class Router:
    def __init__(self, rng, index, interfaces, boot_time, legacy, contents_name):
        self.index = index
        self.contents_name = contents_name
        self.hostname = f"router-{index:03d}.lab.example.net"
        self.legacy = legacy
        self.sequence = 0
        self.interfaces = [Interface(rng, i, boot_time) for i in range(interfaces)]
        self.vrfs = {
            "default": [BgpNeighbor(rng, f"192.0.2.{rng.randrange(1, 255)}", 64500 + i) for i in range(3)]
                       + [BgpNeighbor(rng, f"2001:db8:ffff::{i + 1:x}", 64510 + i) for i in range(2)],
            "CUSTOMER-A": [BgpNeighbor(rng, f"198.51.100.{rng.randrange(1, 255)}", 65001)],
        }

    def wrap(self, ts, notification):
        self.sequence += 1
        if self.legacy:
            return {"ietf-restconf:notification": {"eventTime": iso(ts), **notification}}
        return {
            "ietf-yp-notification:envelope": {
                "event-time": iso(ts),
                "hostname": self.hostname,
                "sequence-number": self.sequence,
                self.contents_name: notification,
            }
        }

    def push_update(self, ts, sub_id, contents):
        return self.wrap(ts, {"ietf-yang-push:push-update": {"id": sub_id, "datastore-contents": contents}})

    def tick(self, tick, ts, interval):
        """Yields (kind, subscription id, message) for one tick."""
        for interface in self.interfaces:
            if interface.advance(interval):
                edit = {
                    "edit-id": "edit-1",
                    "operation": "merge",
                    "target": "/ietf-interfaces:interfaces/interface=" + quote(interface.name, safe=""),
                    "value": {"ietf-interfaces:interface": [
                        {"name": interface.name, "oper-status": "up" if interface.up else "down"}
                    ]},
                }
                yield "on-change", 3000 + self.index, self.wrap(ts, {"ietf-yang-push:push-change-update": {
                    "id": 3000 + self.index,
                    "datastore-changes": {"yang-patch": {"patch-id": f"p-{self.sequence + 1}", "edit": [edit]}},
                }})
        for neighbors in self.vrfs.values():
            for neighbor in neighbors:
                neighbor.advance()

        yield "stats-subtree", 4000 + self.index, self.push_update(ts, 4000 + self.index, {"ietf-interfaces:interfaces": {
            "interface": [{"name": i.name, "statistics": i.statistics()} for i in self.interfaces]
        }})
        if tick % FULL_EVERY == 0:
            yield "interfaces-full", 1000 + self.index, self.push_update(ts, 1000 + self.index, {"ietf-interfaces:interfaces": {
                "interface": [i.full() for i in self.interfaces]
            }})
        if tick % BGP_EVERY == 0:
            yield "bgp", 2000 + self.index, self.push_update(ts, 2000 + self.index, {"openconfig-network-instance:network-instances": {
                "network-instance": [
                    {"name": vrf, "protocols": {"protocol": [{
                        "identifier": "openconfig-policy-types:BGP",
                        "name": "BGP",
                        "bgp": {"neighbors": {"neighbor": [n.to_oc() for n in neighbors]}},
                    }]}}
                    for vrf, neighbors in self.vrfs.items()
                ]
            }})


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--routers", type=int, default=10)
    parser.add_argument("--legacy-routers", type=int, default=2,
                        help="how many routers use the RFC 8641 envelope without hostname")
    parser.add_argument("--interfaces", type=int, default=12, help="interfaces per router")
    parser.add_argument("--interval", type=float, default=10.0, help="seconds per tick")
    parser.add_argument("--backfill", type=float, default=0.0,
                        help="first emit this many hours of history as fast as possible, then continue in real time")
    parser.add_argument("--ticks", type=int, default=0, help="stop after this many ticks (0 = run forever)")
    parser.add_argument("--key", action="store_true", help="prefix each line with its hostname and a tab")
    parser.add_argument("--envelope-contents", choices=["notification-contents", "contents"],
                        default="notification-contents",
                        help="member name holding the notification in the notif-envelope (the draft has used both)")
    parser.add_argument("--kinds", default=",".join(KINDS), help="comma-separated kinds to emit")
    parser.add_argument("--bootstrap", help="produce directly to this Kafka bootstrap server")
    parser.add_argument("--topic", help="topic for --bootstrap")
    parser.add_argument("--schema-id", action="append", default=[], metavar="KIND=ID",
                        help="with --bootstrap, send this schema id in the schema-id header for KIND")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    kinds = set(args.kinds.split(","))
    schema_ids = dict((k, int(v)) for k, v in (x.split("=", 1) for x in args.schema_id))
    producer = None
    if args.bootstrap:
        from kafka import KafkaProducer  # pip install kafka-python
        producer = KafkaProducer(bootstrap_servers=args.bootstrap, linger_ms=20)

    rng = random.Random(args.seed)
    start = time.time() - args.backfill * 3600
    routers = [
        Router(rng, i + 1, args.interfaces, start - 86400, legacy=i >= args.routers - args.legacy_routers,
               contents_name=args.envelope_contents)
        for i in range(args.routers)
    ]

    counts = collections.Counter()
    reported_backfill = args.backfill == 0

    def report(label):
        sys.stderr.write(f"[{label}] messages by kind: {dict(sorted(counts.items()))}\n")
        sys.stderr.flush()

    tick = 0
    ts = start
    try:
        while not args.ticks or tick < args.ticks:
            for router in routers:
                for kind, sub_id, message in router.tick(tick, ts, args.interval):
                    if kind not in kinds:
                        continue
                    payload = json.dumps(message, separators=(",", ":"))
                    if producer:
                        headers = [("content-type", b"application/yang-data+json")]
                        if kind in schema_ids:
                            headers.append(("schema-id", struct.pack(">i", schema_ids[kind])))
                        key = f"{router.hostname}\n{sub_id}\n{XPATHS[kind]}"
                        producer.send(args.topic, key=key.encode(), value=payload.encode(), headers=headers)
                    else:
                        if args.key:
                            sys.stdout.write(router.hostname + "\t")
                        sys.stdout.write(payload)
                        sys.stdout.write("\n")
                    counts[kind] += 1
            if producer:
                producer.flush()
            else:
                sys.stdout.flush()
            tick += 1
            ts += args.interval
            delay = ts - time.time()
            if delay > 0:
                if not reported_backfill:
                    report("backfill done")
                    reported_backfill = True
                time.sleep(delay)
    finally:
        report("exit")


if __name__ == "__main__":
    try:
        main()
    except (BrokenPipeError, KeyboardInterrupt):
        pass
