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
  The default HTTP fetcher pauses all fetches 10 min (doubling) after 5 in 60 s.

See docs/columnar-index.adoc, "Bandwidth: threads and pruning" and "Rate limiting".
