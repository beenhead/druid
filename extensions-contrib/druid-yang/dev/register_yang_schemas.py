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
Registers YANG modules in a Confluent schema registry running the network-analytics YANG plugin, following
draft-ietf-nmop-yang-message-broker-integration: one subject per module, with its imports as references.

Optionally also registers a subscription schema: the subscribed module under its own subject, with the modules that
augment it listed as references as well, and prints its schema id (the value YANG-Push producers put in the
"schema-id" message header).

  python3 register_yang_schemas.py --url http://localhost:18081 --modules ../src/test/resources/yang/modules \\
      --subscription interfaces-push=ietf-interfaces --augments ietf-ip,acme-if-ext
"""

import argparse
import json
import pathlib
import re
import sys
import urllib.error
import urllib.request

MODULE = re.compile(r"^\s*module\s+([A-Za-z_][\w.-]*)", re.M)
IMPORT = re.compile(r"^\s*import\s+([A-Za-z_][\w.-]*)", re.M)
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def call(url, method="GET", body=None):
    request = urllib.request.Request(url, method=method, headers={"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE})
    if body is not None:
        request.data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {url} failed: HTTP {e.code} {e.read().decode()}")


def register(base, subject, schema, references):
    refs = [{"name": r, "subject": r, "version": call(f"{base}/subjects/{r}/versions/latest")["version"]}
            for r in references]
    result = call(f"{base}/subjects/{subject}/versions", "POST",
                  {"schemaType": "YANG", "schema": schema, "references": refs})
    print(f"  {subject:24} id {result['id']:<4} refs {references}")
    return result["id"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:18081")
    parser.add_argument("--modules", required=True, help="directory of .yang files")
    parser.add_argument("--subscription", help="SUBJECT=MODULE: register MODULE again as SUBJECT with its augments")
    parser.add_argument("--augments", default="", help="comma-separated modules augmenting the subscribed module")
    args = parser.parse_args()
    base = args.url.rstrip("/")

    modules = {}
    for path in sorted(pathlib.Path(args.modules).glob("*.yang")):
        text = path.read_text()
        modules[MODULE.search(text).group(1)] = text
    imports = {name: [i for i in IMPORT.findall(text) if i in modules] for name, text in modules.items()}

    print("modules:")
    registered = set()
    while len(registered) < len(modules):
        ready = [m for m in sorted(modules) if m not in registered and all(i in registered for i in imports[m])]
        if not ready:
            sys.exit(f"unresolvable imports among {sorted(set(modules) - registered)}")
        for name in ready:
            register(base, name, modules[name], imports[name])
            registered.add(name)

    if args.subscription:
        subject, module = args.subscription.split("=", 1)
        augments = [a for a in args.augments.split(",") if a]
        print("subscription:")
        schema_id = register(base, subject, modules[module], imports[module] + augments)
        print(f"schema-id={schema_id}")


if __name__ == "__main__":
    main()
