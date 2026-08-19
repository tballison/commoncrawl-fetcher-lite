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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import org.tallison.cc.index.CCIndexReaderCounter;

import org.apache.tika.exception.TikaConfigException;

/**
 * "QueryIndex" command -- an alternative to {@link CCFileExtractor} that finds matching files
 * by querying Common Crawl's columnar (Parquet) index directly instead of downloading and
 * scanning the full CDX index shard-by-shard. See docs/columnar-index.adoc.
 *
 * <p>Requires AWS credentials (any valid account -- this is NOT AWS Athena and incurs no Athena
 * charges) because the Common Crawl S3 bucket allows anonymous {@code GetObject} but not
 * anonymous {@code ListBucket}, and listing the Parquet part-files under a crawl/subset
 * partition requires the latter. The bucket is not requester-pays, so the list/get calls
 * themselves are free regardless of whose credentials sign them.
 *
 * <p>Each matched row is translated into a synthetic CDX-format line and fed through the same
 * {@link CCFileExtractorRecordProcessor} that {@link CCFileExtractor} uses, so WARC fetching,
 * CSV logging, {@code dryRun}, {@code maxFilesExtracted}/{@code maxFilesTruncated}, and
 * {@link RunLabel} log scoping all behave identically to the CDX-scanning path.
 */
public class CCColumnarIndexExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CCColumnarIndexExtractor.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ExtractorConfig fetcherConfig =
                new ObjectMapper().readValue(new File(args[0]), ExtractorConfig.class);
        execute(fetcherConfig);
    }

    private static void execute(ExtractorConfig fetcherConfig) throws Exception {
        ExtractorConfig.ColumnarIndexConfig columnarConfig =
                fetcherConfig.getColumnarIndexConfig();
        if (columnarConfig == null) {
            throw new TikaConfigException(
                    "config requires a \"columnarIndex\" section -- see "
                            + "docs/columnar-index.adoc");
        }

        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        CCFileExtractorRecordProcessor processor =
                new CCFileExtractorRecordProcessor(fetcherConfig, counter);

        String sql = "SELECT url, content_mime_type, content_mime_detected, fetch_status, "
                + "content_digest, content_charset, content_languages, content_truncated, "
                + "warc_filename, warc_record_offset, warc_record_length "
                + "FROM read_parquet('" + columnarConfig.getParquetGlobPath()
                + "', hive_partitioning=1) WHERE " + columnarConfig.getWhere();

        LOGGER.info("querying columnar index: {}", sql);
        long start = System.currentTimeMillis();
        try {
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
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
                }
                try (Statement query = conn.createStatement();
                        ResultSet rs = query.executeQuery(sql)) {
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
                }
            }
        } finally {
            processor.close();
        }
        long elapsed = System.currentTimeMillis() - start;
        LOGGER.info(
                "Finished querying columnar index in ({}) ms: {}",
                String.format(Locale.US, "%,d", elapsed),
                counter);
    }

    /**
     * Builds a synthetic CDX-format line ("sortkey timestamp {json}") from a columnar-index
     * result row, using placeholder sortkey/timestamp tokens (unused by
     * {@link org.tallison.cc.index.CCIndexRecord#parseRecord}) and a real JSON payload with the
     * same kebab-case keys as an actual CDX line, so it flows through the existing CDX-parsing
     * and fetch pipeline unchanged.
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
