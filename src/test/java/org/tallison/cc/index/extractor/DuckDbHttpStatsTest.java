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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.tallison.cc.index.io.RateLimitMonitor;

/** DuckDB against a loopback server that always answers 503: proves its retries are visible. */
public class DuckDbHttpStatsTest {

    @Test
    public void testParseStatus() {
        assertEquals(503, DuckDbHttpStats.parseStatus("ServiceUnavailable_503"));
        assertEquals(429, DuckDbHttpStats.parseStatus("TooManyRequests_429"));
        assertEquals(-1, DuckDbHttpStats.parseStatus(null));
    }

    @Test
    public void testCountsDuckDbRetries() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                ex -> {
                    byte[] b = "<Code>SlowDown</Code>".getBytes(StandardCharsets.US_ASCII);
                    ex.sendResponseHeaders(503, b.length);
                    ex.getResponseBody().write(b);
                    ex.close();
                });
        server.start();
        RateLimitMonitor m = new RateLimitMonitor(System::currentTimeMillis, 3, 60_000, 600_000);
        try (DuckDBConnection conn =
                        (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                Statement st = conn.createStatement()) {
            st.execute("LOAD httpfs; SET http_retries = 3; SET http_retry_wait_ms = 10;");
            DuckDbHttpStats stats = DuckDbHttpStats.start(conn, st, m, 0);
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/x.parquet";
            assertThrows(
                    SQLException.class,
                    () -> st.executeQuery("SELECT count(*) FROM read_parquet('" + url + "')"));
            stats.close();
            // one attempt plus three retries, each logged
            assertEquals(Map.of(503, 4L), stats.rateLimitedByStatus());
            assertEquals(4, stats.getTotalRequests());
            assertEquals(4, m.getTotal());
            assertTrue(stats.isTripped());
        } finally {
            server.stop(0);
        }
    }
    @Test
    public void testClearingTheLogKeepsCounts() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                ex -> {
                    ex.sendResponseHeaders(503, -1);
                    ex.close();
                });
        server.start();
        RateLimitMonitor m = new RateLimitMonitor(System::currentTimeMillis, 100, 60_000, 600_000);
        try (DuckDBConnection conn =
                        (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                Statement st = conn.createStatement()) {
            st.execute("LOAD httpfs; SET http_retries = 3; SET http_retry_wait_ms = 10;");
            DuckDbHttpStats stats = DuckDbHttpStats.start(conn, st, m, 0);
            stats.truncateAt = 2;
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/x.parquet";
            for (int i = 0; i < 2; i++) {
                // DuckDB's JDBC closes a statement that failed
                Statement q = conn.createStatement();
                assertThrows(
                        SQLException.class,
                        () -> q.executeQuery("SELECT count(*) FROM read_parquet('" + url + "')"));
                stats.poll();
                try (Statement c = conn.createStatement();
                        ResultSet rs =
                                c.executeQuery("SELECT count(*) FROM duckdb_logs_parsed('HTTP')")) {
                    rs.next();
                    assertEquals(0, rs.getLong(1), "log cleared after each poll");
                }
            }
            stats.close();
            assertEquals(Map.of(503, 8L), stats.rateLimitedByStatus());
            assertEquals(8, stats.getTotalRequests());
            assertEquals(8, m.getTotal());
        } finally {
            server.stop(0);
        }
    }
}
