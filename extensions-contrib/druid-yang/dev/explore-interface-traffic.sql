-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Source query for the Druid console's Explore view: interface traffic from YANG-Push ietf-interfaces counters.
--
-- Rows are per interface per minute, with rates in bits per second; the --:MEASURE lines define Explore measures
-- that aggregate those rows correctly for any time bucket and split.
SELECT
  *
  --:MEASURE SUM(in_bps) / COUNT(DISTINCT __time) / 1e9 AS "Total in (Gbps)"
  --:MEASURE SUM(out_bps) / COUNT(DISTINCT __time) / 1e9 AS "Total out (Gbps)"
  --:MEASURE AVG(in_bps) / 1e9 AS "Avg in per interface (Gbps)"
  --:MEASURE AVG(out_bps) / 1e9 AS "Avg out per interface (Gbps)"
  --:MEASURE MAX(GREATEST(in_bps, out_bps) / speed) * 100 AS "Peak utilisation (%)"
  --:MEASURE SUM(in_errors) AS "Input errors"
  --:MEASURE COUNT(DISTINCT device || ' ' || name) AS "Interfaces"
FROM (
  -- One row per interface per minute. Counters only increase (until a reset), so MAX - MIN over the minute's
  -- samples divided by the time between the first and last sample is the interface's average rate.
  SELECT
    TIME_FLOOR(__time, 'PT1M') AS __time,
    device,
    name,
    (MAX(in_octets) - MIN(in_octets)) * 8.0 / TIMESTAMPDIFF(SECOND, MIN(__time), MAX(__time)) AS in_bps,
    (MAX(out_octets) - MIN(out_octets)) * 8.0 / TIMESTAMPDIFF(SECOND, MIN(__time), MAX(__time)) AS out_bps,
    MAX(in_errors) - MIN(in_errors) AS in_errors,
    MAX(speed) AS speed
  FROM yang_telemetry
  WHERE "yang-rule" = 'interfaces'
    AND "yang-update" = 'push-update'
    AND in_octets IS NOT NULL
    -- Explore's time filter is applied outside this subquery and does not limit the scan, so bound it here.
    AND __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
  GROUP BY 1, 2, 3
  HAVING MAX(__time) > MIN(__time)
    AND MAX(in_octets) >= MIN(in_octets)
)
