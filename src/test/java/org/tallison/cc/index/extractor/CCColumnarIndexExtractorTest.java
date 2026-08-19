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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;

import org.tallison.cc.index.CCIndexRecord;

/**
 * Verifies the columnar-index-row -&gt; CDX-line -&gt; {@link CCIndexRecord} round trip using an
 * in-memory DuckDB table (no network/AWS access), so it runs in CI like any other unit test.
 */
public class CCColumnarIndexExtractorTest {

    @Test
    public void testFullRowRoundTrips() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE t AS SELECT "
                            + "'https://example.com/notes.one' AS url, "
                            + "'application/octet-stream' AS content_mime_type, "
                            + "'application/onenote; format=one' AS content_mime_detected, "
                            + "200 AS fetch_status, "
                            + "'ABC123' AS content_digest, "
                            + "'UTF-8' AS content_charset, "
                            + "'eng' AS content_languages, "
                            + "CAST(NULL AS VARCHAR) AS content_truncated, "
                            + "'crawl-data/CC-MAIN-2026-30/segments/x/warc/y.warc.gz' "
                            + "AS warc_filename, "
                            + "1234 AS warc_record_offset, "
                            + "5678 AS warc_record_length");
            try (ResultSet rs = st.executeQuery("SELECT * FROM t")) {
                assertTrue(rs.next());
                String line = CCColumnarIndexExtractor.toCdxLine(rs);

                Optional<CCIndexRecord> parsed = CCIndexRecord.parseRecord(line);
                assertTrue(parsed.isPresent(), "line should parse: " + line);
                CCIndexRecord r = parsed.get();
                assertEquals("https://example.com/notes.one", r.getUrl());
                assertEquals("application/octet-stream", r.getMime());
                assertEquals("application/onenote; format=one", r.getMimeDetected());
                assertEquals(200, r.getStatus());
                assertEquals("ABC123", r.getDigest());
                assertEquals("UTF-8", r.getCharset());
                assertEquals("eng", r.getLanguages());
                assertEquals(
                        "crawl-data/CC-MAIN-2026-30/segments/x/warc/y.warc.gz", r.getFilename());
                assertEquals(1234, r.getOffset());
                assertEquals(5678L, r.getLength());
            }
        }
    }

    @Test
    public void testNullColumnsOmittedNotCrash() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE t AS SELECT "
                            + "'https://example.com/x.one' AS url, "
                            + "CAST(NULL AS VARCHAR) AS content_mime_type, "
                            + "'application/onenote; format=one' AS content_mime_detected, "
                            + "CAST(NULL AS SMALLINT) AS fetch_status, "
                            + "CAST(NULL AS VARCHAR) AS content_digest, "
                            + "CAST(NULL AS VARCHAR) AS content_charset, "
                            + "CAST(NULL AS VARCHAR) AS content_languages, "
                            + "'length' AS content_truncated, "
                            + "'crawl-data/x/y.warc.gz' AS warc_filename, "
                            + "CAST(NULL AS INTEGER) AS warc_record_offset, "
                            + "CAST(NULL AS INTEGER) AS warc_record_length");
            try (ResultSet rs = st.executeQuery("SELECT * FROM t")) {
                assertTrue(rs.next());
                String line = CCColumnarIndexExtractor.toCdxLine(rs);

                Optional<CCIndexRecord> parsed = CCIndexRecord.parseRecord(line);
                assertTrue(parsed.isPresent(), "line should parse: " + line);
                CCIndexRecord r = parsed.get();
                assertEquals("https://example.com/x.one", r.getUrl());
                assertEquals("length", r.getTruncated());
            }
        }
    }
}
