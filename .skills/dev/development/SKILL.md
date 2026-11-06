---
name: development
description: >
  Building, testing and the code layout of commoncrawl-fetcher-lite. Load before changing the code.
---

# Development

## Build & run

```bash
# Build (creates fat JAR in target/)
./mvnw package -DskipTests

# Run with a config file
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar config.json

# Or with explicit command
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar Fetch config.json

# Quick fetch without writing a config file
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar QuickFetch \
  --mime "application/pdf" --output ~/data/pdfs --crawl latest

# List available crawls
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar ListCrawls

# Other commands
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar CountMimes config.json
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar FetchIndices config.json
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar --help

# Option B: query the columnar (Parquet) index instead of scanning CDX shards -- much
# faster, can span many/all crawls in one query, needs AWS credentials (not Athena, not
# billed -- see docs/columnar-index.adoc). Config uses "columnarIndex" instead of "indices".
java -jar target/commoncrawl-fetcher-lite-1.0.0-SNAPSHOT.jar QueryIndex config.json
```

## Architecture

- **Entry point**: `CCFetcherCli.java` -- dispatches to command classes
- **Config**: `ExtractorConfig.java` -- Jackson POJO deserialized from JSON config
- **Commands**: `CCFileExtractor` (Fetch), `CCColumnarIndexExtractor` (QueryIndex -- Option B, queries the columnar/Parquet index via DuckDB JDBC instead of scanning CDX), `CCIndexFetcher` (FetchIndices), `CCMimeCounter` (CountMimes), `CCQuickFetch` (QuickFetch), `CCListCrawls` (ListCrawls)
- **Selection**: `RecordSelector` with `MatchSelector`, `RegexSelector`, `ExtensionsSelector`
- **Index resolution**: `IndexIterator` -- resolves `cc-index.paths.gz` lists to individual `cdx-*.gz` files

## Testing

```bash
./mvnw test
```
