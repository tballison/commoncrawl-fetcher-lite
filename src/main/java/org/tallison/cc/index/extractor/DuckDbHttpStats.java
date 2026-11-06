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

import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tallison.cc.index.io.RateLimitMonitor;

/**
 * Surfaces DuckDB httpfs retries. DuckDB logs every HTTP attempt (retries included) when logging
 * is on; a side connection polls those logs during the query, feeds 503/429s into a {@link
 * RateLimitMonitor} and cancels the query when it trips. DuckDB 1.3 has no log truncation call, so
 * every {@link #TRUNCATE_AT} logged requests the counts are banked and the log is cleared by
 * switching its storage (requests in that brief window are not counted). {@code
 * -Dcc.duckdb.httpStats=false} turns this off.
 */
class DuckDbHttpStats implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DuckDbHttpStats.class);

    // bank the counts and clear DuckDB's in-memory log past this many entries
    static final long TRUNCATE_AT = 20_000;

    private static final Pattern STATUS = Pattern.compile("(\\d{3})$");

    private static final String SNAPSHOT_SQL =
            "SELECT CAST(response.status AS VARCHAR) AS status, "
                    + "regexp_extract(request.url, '^[a-z0-9]+://([^/:?]+)', 1) AS host, "
                    + "count(*) AS n FROM duckdb_logs_parsed('HTTP') GROUP BY ALL";

    private final Connection side;
    private final Statement query;
    private final RateLimitMonitor monitor;
    private final ScheduledExecutorService poller;
    // "host status" -> count: banked from cleared logs, and last seen in the live log
    private final Map<String, Long> banked = new HashMap<>();
    private final Map<String, Long> seen = new HashMap<>();
    private long bankedRequests = 0;
    private long totalRequests = 0;
    private volatile boolean tripped = false;
    long truncateAt = TRUNCATE_AT;

    private DuckDbHttpStats(
            Connection side, Statement query, RateLimitMonitor monitor, long pollMs) {
        this.side = side;
        this.query = query;
        this.monitor = monitor;
        if (pollMs > 0) {
            poller = Executors.newSingleThreadScheduledExecutor();
            poller.scheduleWithFixedDelay(this::pollQuietly, pollMs, pollMs, TimeUnit.MILLISECONDS);
        } else {
            poller = null;
        }
    }

    static boolean isEnabled() {
        return !"false".equalsIgnoreCase(System.getProperty("cc.duckdb.httpStats", "true"));
    }

    static void enableLogging(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("SET enable_logging = true;");
            st.execute("SET logging_level = 'DEBUG';");
            st.execute("SET enabled_log_types = 'HTTP';");
        }
    }

    /**
     * Starts polling DuckDB's HTTP log; {@code query} is cancelled if 503/429s trip {@code
     * monitor}. Returns null if disabled.
     */
    static DuckDbHttpStats start(
            DuckDBConnection conn, Statement query, RateLimitMonitor monitor, long pollMs)
            throws SQLException {
        if (!isEnabled()) {
            return null;
        }
        enableLogging(conn);
        return new DuckDbHttpStats(conn.duplicate(), query, monitor, pollMs);
    }

    boolean isTripped() {
        return tripped;
    }

    long getTotalRequests() {
        return totalRequests;
    }

    private void pollQuietly() {
        try {
            poll();
        } catch (SQLException | RuntimeException e) {
            LOGGER.debug("DuckDB HTTP stats poll failed", e);
        }
    }

    synchronized void poll() throws SQLException {
        long live = snapshot();
        totalRequests = bankedRequests + live;
        if (live >= truncateAt) {
            truncate();
        }
    }

    /** Reads the live log, feeds new 503/429s to the monitor; returns the live entry count. */
    private long snapshot() throws SQLException {
        long total = 0;
        try (Statement st = side.createStatement();
                ResultSet rs = st.executeQuery(SNAPSHOT_SQL)) {
            while (rs.next()) {
                long n = rs.getLong("n");
                total += n;
                int status = parseStatus(rs.getString("status"));
                if (!RateLimitMonitor.isRateLimitStatus(status)) {
                    continue;
                }
                String host = rs.getString("host");
                String k = host + " " + status;
                long delta = n - seen.getOrDefault(k, 0L);
                seen.put(k, n);
                if (monitor.record("DuckDB " + host, status, delta) && !tripped) {
                    tripped = true;
                    LOGGER.error("cancelling the DuckDB query: Common Crawl is rate-limiting");
                    try {
                        query.cancel();
                    } catch (SQLException e) {
                        LOGGER.warn("cancel failed", e);
                    }
                }
            }
        }
        return total;
    }

    /** Banks the live counts and clears the log (logging off while storage is swapped). */
    private void truncate() throws SQLException {
        try (Statement st = side.createStatement()) {
            st.execute("SET enable_logging = false;");
            long live = snapshot();
            st.execute("SET logging_storage = 'stdout';");
            st.execute("SET logging_storage = 'memory';");
            st.execute("SET enable_logging = true;");
            bankedRequests += live;
            for (Map.Entry<String, Long> e : seen.entrySet()) {
                banked.merge(e.getKey(), e.getValue(), Long::sum);
            }
            seen.clear();
            totalRequests = bankedRequests;
            LOGGER.debug("DuckDB HTTP log cleared after {} requests", bankedRequests);
        }
    }

    /** "ServiceUnavailable_503" -> 503; -1 if absent (no response). */
    static int parseStatus(String s) {
        if (s == null) {
            return -1;
        }
        Matcher m = STATUS.matcher(s);
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    Map<Integer, Long> rateLimitedByStatus() {
        Map<Integer, Long> out = new TreeMap<>();
        for (Map<String, Long> m : List.of(banked, seen)) {
            for (Map.Entry<String, Long> e : m.entrySet()) {
                String k = e.getKey();
                int status = Integer.parseInt(k.substring(k.lastIndexOf(' ') + 1));
                out.merge(status, e.getValue(), Long::sum);
            }
        }
        return out;
    }

    /** Stops polling, takes a final count and logs the summary. */
    @Override
    public synchronized void close() {
        if (poller != null) {
            poller.shutdownNow();
        }
        try {
            poll();
        } catch (SQLException e) {
            LOGGER.warn("couldn't read DuckDB HTTP stats", e);
        }
        long limited = rateLimitedByStatus().values().stream().mapToLong(Long::longValue).sum();
        if (limited == 0) {
            LOGGER.info("DuckDB HTTP: {} requests, no 503/429 retries", totalRequests);
        } else {
            LOGGER.warn(
                    "DuckDB HTTP: {} requests, {} rate-limited retries {}{}",
                    totalRequests,
                    limited,
                    rateLimitedByStatus(),
                    limited >= RateLimitMonitor.DEFAULT_THRESHOLD
                            ? " -- high: Common Crawl is throttling this host; slow down"
                            : "");
        }
        try {
            side.close();
        } catch (SQLException e) {
            LOGGER.debug("close", e);
        }
    }
}
