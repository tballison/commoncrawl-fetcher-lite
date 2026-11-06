---
name: columnar-query
description: >
  Querying the Common Crawl columnar (Parquet) index with QueryIndex/CountIndex via DuckDB: bandwidth limits, SURT-range pruning, measuring cost first. Load before any QueryIndex or CountIndex run.
---

# Columnar index queries (QueryIndex, CountIndex)

Full reference: docs/columnar-index.adoc (config, column reference, retry tuning, provenance).

## Network etiquette (read before running QueryIndex or CountIndex)

A `QueryIndex` scan streams Parquet from S3 through whatever machine runs it. On 2026-11-04 two
concurrent scans with DuckDB's default threads (all cores) saturated a laptop's internet link for
hours and forced a session restart. Rules:

- Always pass `-Dcc.duckdb.threads=1` (2 at most) on a workstation. Config `numThreads` is the WARC
  fetch pool, not the scan.
- Run one `QueryIndex`/`CountIndex` at a time.
- Bound each query to one TLD (`url_host_tld = 'xx'`) so row groups are skipped; the index is SURT-sorted
  (a `url_surtkey` range adds cost, not pruning). Predicates without a TLD bound read the needed
  columns for the whole crawl -- run those only with the user's approval, single-threaded, overnight.
- Measure first: `CountIndex` on a small TLD, noting wall time and bytes received.

## If everything is slow, check for 503 SlowDown

Slow queries (hundreds of KB/s) usually mean Common Crawl is rate-limiting this IP: 503 SlowDown
(or 429), per IP, on data.commoncrawl.org and S3 alike. DuckDB's widened retries hide it. It is a
request to slow down:

- Stop. Wait (hours), then resume at a lower request rate (`-Dcc.duckdb.threads=1`, Fetch
  `numThreads` 1).
- Do not move to another host or IP to get around it.
- Look for the ERROR banner "Common Crawl is rate-limiting this host (503 SlowDown)", WARN lines
  "rate-limited by ...", and `rate_limited` in `logs/<runLabel>/manifest.json`.
- QueryIndex/CountIndex preflight-HEAD a small object and refuse to start on 503/429
  (`-Dcc.preflight=false` overrides); during a query, 5 DuckDB 503/429 retries in 60 s cancel it.
  DuckDB's HTTP log is cleared as it goes, so crawl-wide scans stay counted (no 200k cap). But in-region
  S3 SlowDown retries went uncounted on 2026-11-05: a query that stops reading with no sockets open is
  DuckDB backing off -- treat it as rate-limiting. DuckDB retries each failed read 12 times, waiting
  2 s x 1.5^n (~9 min total, longest ~3 min); `-Dcc.duckdb.httpRetries`, `httpRetryWaitMs`,
  `httpRetryBackoff` override. For unattended runs, gate each attempt on a 1 KB GetObject probe of
  an index file and sleep when it returns SlowDown.
- Fetch over HTTP or S3 (`fetcher.profile`): 5 503/429s in 60 s from any thread pause every fetch
  thread 10 min (doubling); S3 SlowDowns count under `s3://commoncrawl` in `manifest.json`.

## In-region (EC2 us-east-1, S3)

The laptop limits above are for a home link. In-region the scan is latency-bound: DuckDB threads 8
on 4 vCPUs read ~48 MB/s with no 503s (2 threads: ~14 MB/s). Fetch `numThreads` 4 to start (on
2026-11-05, 16 drew ~30 SlowDowns/min and 8 and 4 tripped the pause; 2 did not); halve on a
pause, don't move hosts. Fetch runs one worker per CDX file, so split a CDX batch into >= numThreads files.

See docs/columnar-index.adoc, "Bandwidth: threads and pruning" and "Rate limiting".
