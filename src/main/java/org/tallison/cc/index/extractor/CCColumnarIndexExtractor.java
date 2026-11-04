/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.cc.index.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.io.RateLimitMonitor;
import org.tallison.cc.index.io.RateLimitPreflight;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.utils.StringUtils;

/**
 * "QueryIndex" command -- an alternative to {@link CCFileExtractor} that finds matching files by
 * querying Common Crawl's columnar (Parquet) index directly instead of downloading and scanning the
 * full CDX index shard-by-shard. See docs/columnar-index.adoc.
 *
 * <p>Requires AWS credentials (any valid account -- this is NOT AWS Athena and incurs no Athena
 * charges) because the Common Crawl S3 bucket allows anonymous {@code GetObject} but not anonymous
 * {@code ListBucket}, and listing the Parquet part-files under a crawl/subset partition requires
 * the latter. The bucket is not requester-pays, so the list/get calls themselves are free
 * regardless of whose credentials sign them.
 *
 * <p>Each matched row is translated into a synthetic CDX-format line and fed through the same
 * {@link CCFileExtractorRecordProcessor} that {@link CCFileExtractor} uses, so WARC fetching, CSV
 * logging, {@code dryRun}, {@code maxFilesExtracted}/{@code maxFilesTruncated}, and {@link
 * RunLabel} log scoping all behave identically to the CDX-scanning path.
 */
public class CCColumnarIndexExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCColumnarIndexExtractor.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final long STATS_POLL_MS = 30_000;

    public static void main(String[] args) throws Exception {
        execute(ExtractorConfig.load(new File(args[0])));
    }

    /**
     * "CountIndex" command: runs columnarIndex.countQuery and prints the table; fetches nothing.
     */
    public static void count(String[] args) throws Exception {
        ExtractorConfig config = ExtractorConfig.load(new File(args[0]));
        ExtractorConfig.ColumnarIndexConfig columnarConfig = requireColumnar(config);
        if (StringUtils.isBlank(columnarConfig.getCountQuery())) {
            throw new TikaConfigException("CountIndex requires columnarIndex.countQuery");
        }
        String sql = countSql(columnarConfig);
        LOGGER.info("count query: {}", sql);
        RateLimitPreflight.check(columnarConfig.getCrawlGlob());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            setup(conn);
            try (Statement st = conn.createStatement();
                    DuckDbHttpStats stats = startStats(conn, st)) {
                ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
                timer.schedule(
                        () -> {
                            LOGGER.warn("count query timed out, cancelling");
                            try {
                                st.cancel();
                            } catch (SQLException e) {
                                LOGGER.warn("cancel failed", e);
                            }
                        },
                        columnarConfig.getCountTimeoutSeconds(),
                        TimeUnit.SECONDS);
                try (ResultSet rs = st.executeQuery(sql)) {
                    System.out.println(formatTable(rs));
                } catch (SQLException e) {
                    throw rateLimitedOr(stats, e);
                } finally {
                    timer.shutdownNow();
                }
            }
        }
    }

    static String countSql(ExtractorConfig.ColumnarIndexConfig c) {
        return c.getCountQuery()
                .replace(
                        "${parquet}",
                        "read_parquet('" + c.getParquetGlobPath() + "', hive_partitioning=1)");
    }

    /** Tab-separated table: header row then one row per result row. */
    static String formatTable(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            sb.append(i > 1 ? "\t" : "").append(md.getColumnLabel(i));
        }
        sb.append('\n');
        while (rs.next()) {
            for (int i = 1; i <= n; i++) {
                sb.append(i > 1 ? "\t" : "").append(rs.getString(i));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static ExtractorConfig.ColumnarIndexConfig requireColumnar(ExtractorConfig config)
            throws TikaConfigException {
        ExtractorConfig.ColumnarIndexConfig columnarConfig = config.getColumnarIndexConfig();
        if (columnarConfig == null) {
            throw new TikaConfigException(
                    "config requires a \"columnarIndex\" section -- see "
                            + "docs/columnar-index.adoc");
        }
        return columnarConfig;
    }

    private static void setup(Connection conn) throws SQLException {
        try (Statement setup = conn.createStatement()) {
            setup.execute("INSTALL httpfs; LOAD httpfs;");
            setup.execute("INSTALL aws; LOAD aws;");
            setup.execute("CALL load_aws_credentials();");
            setup.execute("SET s3_region='" + ExtractorConfig.CC_REGION + "';");
            // DuckDB's httpfs defaults (http_retries=3, http_retry_wait_ms=100,
            // http_retry_backoff=4) give well under 2 seconds of total backoff --
            // nowhere near enough to survive S3 SlowDown throttling on a broad
            // crawlGlob (e.g. "CC-MAIN-*") touching thousands of files across many
            // crawls. Empirically, the httpfs defaults gave up on such a query after
            // ~97 minutes to a 503 SlowDown with no partial results retained. Widen
            // the retry budget to something in the spirit of BackoffHttpFetcher's
            // ladder (this doesn't touch that class -- DuckDB's S3 reads for the
            // Parquet index are a separate HTTP client from the WARC/CDX fetches).
            setup.execute("SET http_retries = 12;");
            setup.execute("SET http_retry_wait_ms = 2000;");
            setup.execute("SET http_retry_backoff = 2;");
            // DuckDB defaults to 80% of RAM; a crawl-wide scan that reads a wide column
            // (e.g. url) can then take the whole box. -Dcc.duckdb.memoryLimit overrides.
            String mem = System.getProperty("cc.duckdb.memoryLimit", "4GB");
            if (!mem.matches("\\d+(\\.\\d+)?\\s*[KMGT]i?B")) {
                throw new SQLException("bad cc.duckdb.memoryLimit: " + mem);
            }
            setup.execute("SET memory_limit='" + mem + "';");
            // Each DuckDB thread issues its own S3 range reads; the default (all cores) can saturate a link.
            String threads = System.getProperty("cc.duckdb.threads");
            if (threads != null) {
                if (!threads.matches("[1-9]\\d*")) {
                    throw new SQLException("bad cc.duckdb.threads: " + threads);
                }
                setup.execute("SET threads=" + threads + ";");
            }
        }
    }

    private static void execute(ExtractorConfig fetcherConfig) throws Exception {
        ExtractorConfig.ColumnarIndexConfig columnarConfig = requireColumnar(fetcherConfig);
        if (StringUtils.isBlank(columnarConfig.getWhere())) {
            throw new TikaConfigException("QueryIndex requires columnarIndex.where");
        }

        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        CCFileExtractorRecordProcessor processor =
                new CCFileExtractorRecordProcessor(fetcherConfig, counter);
        RunManifest manifest = RunManifest.start(fetcherConfig, "QueryIndex");
        String status = "failed";

        String sql =
                "SELECT url, content_mime_type, content_mime_detected, fetch_status, "
                        + "content_digest, content_charset, content_languages, content_truncated, "
                        + "warc_filename, warc_record_offset, warc_record_length "
                        + "FROM read_parquet('"
                        + columnarConfig.getParquetGlobPath()
                        + "', hive_partitioning=1) WHERE "
                        + columnarConfig.getWhere();

        LOGGER.info("querying columnar index: {}", sql);
        long start = System.currentTimeMillis();
        DuckDbHttpStats stats = null;
        try {
            RateLimitPreflight.check(columnarConfig.getCrawlGlob());
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                setup(conn);
                try (Statement query = conn.createStatement()) {
                    stats = startStats(conn, query);
                    runQuery(query, sql, processor, stats);
                } finally {
                    if (stats != null) {
                        stats.close();
                        manifest.setDuckDbHttp(
                                stats.getTotalRequests(), stats.rateLimitedByStatus());
                    }
                }
            }
            status = "completed";
        } finally {
            processor.close();
            manifest.finish(counter, status);
        }
        long elapsed = System.currentTimeMillis() - start;
        LOGGER.info(
                "Finished querying columnar index in ({}) ms: {}",
                String.format(Locale.US, "%,d", elapsed),
                counter);
    }

    private static void runQuery(
            Statement query,
            String sql,
            CCFileExtractorRecordProcessor processor,
            DuckDbHttpStats stats)
            throws SQLException, InterruptedException {
        try (ResultSet rs = query.executeQuery(sql)) {
            while (rs.next()) {
                String line = toCdxLine(rs);
                boolean shouldContinue;
                try {
                    shouldContinue = processor.process(line);
                } catch (IOException e) {
                    LOGGER.warn("bad row: " + line, e);
                    continue;
                }
                if (!shouldContinue) {
                    break;
                }
            }
        } catch (SQLException e) {
            throw rateLimitedOr(stats, e);
        }
    }

    private static DuckDbHttpStats startStats(Connection conn, Statement query)
            throws SQLException {
        return DuckDbHttpStats.start(
                conn.unwrap(DuckDBConnection.class),
                query,
                new RateLimitMonitor(
                        System::currentTimeMillis,
                        RateLimitMonitor.DEFAULT_THRESHOLD,
                        RateLimitMonitor.DEFAULT_WINDOW_MS,
                        RateLimitMonitor.DEFAULT_INITIAL_PAUSE_MS),
                STATS_POLL_MS);
    }

    /** If the stats poller cancelled the query, an exception that says why; else {@code e}. */
    private static SQLException rateLimitedOr(DuckDbHttpStats stats, SQLException e) {
        if (stats == null || !stats.isTripped()) {
            return e;
        }
        return new SQLException(
                "query cancelled: "
                        + RateLimitMonitor.banner(
                                RateLimitMonitor.DEFAULT_THRESHOLD,
                                RateLimitMonitor.DEFAULT_WINDOW_MS),
                e);
    }

    /**
     * Builds a synthetic CDX-format line ("sortkey timestamp {json}") from a columnar-index result
     * row, using placeholder sortkey/timestamp tokens (unused by {@link
     * org.tallison.cc.index.CCIndexRecord#parseRecord}) and a real JSON payload with the same
     * kebab-case keys as an actual CDX line, so it flows through the existing CDX-parsing and fetch
     * pipeline unchanged.
     */
    static String toCdxLine(ResultSet rs) throws SQLException {
        ObjectNode node = MAPPER.createObjectNode();
        putIfPresent(node, "url", rs.getString("url"));
        putIfPresent(node, "mime", rs.getString("content_mime_type"));
        putIfPresent(node, "mime-detected", rs.getString("content_mime_detected"));
        putIntIfPresent(node, "status", rs, "fetch_status");
        putIfPresent(node, "digest", rs.getString("content_digest"));
        putIfPresent(node, "charset", rs.getString("content_charset"));
        putIfPresent(node, "languages", rs.getString("content_languages"));
        putIfPresent(node, "truncated", rs.getString("content_truncated"));
        putIfPresent(node, "filename", rs.getString("warc_filename"));
        putIntIfPresent(node, "offset", rs, "warc_record_offset");
        putIntIfPresent(node, "length", rs, "warc_record_length");
        return "columnar-index 0 " + node;
    }

    private static void putIfPresent(ObjectNode node, String key, String value) {
        if (value != null) {
            node.put(key, value);
        }
    }

    private static void putIntIfPresent(ObjectNode node, String key, ResultSet rs, String column)
            throws SQLException {
        long value = rs.getLong(column);
        if (!rs.wasNull()) {
            node.put(key, String.valueOf(value));
        }
    }
}
