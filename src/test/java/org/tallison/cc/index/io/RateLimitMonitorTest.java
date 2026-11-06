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
package org.tallison.cc.index.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

public class RateLimitMonitorTest {

    private static final long MIN = 60_000;

    @Test
    public void testBelowThresholdNoTrip() {
        AtomicLong now = new AtomicLong(1_000_000);
        RateLimitMonitor m = new RateLimitMonitor(now::get, 5, MIN, 10 * MIN);
        for (int i = 0; i < 4; i++) {
            assertFalse(m.record("h", 503));
        }
        assertEquals(4, m.getTotal());
        assertEquals(0, m.pauseRemainingMs());
        assertEquals(0, m.getTrips());
    }

    @Test
    public void testOldEventsLeaveWindow() {
        AtomicLong now = new AtomicLong(1_000_000);
        RateLimitMonitor m = new RateLimitMonitor(now::get, 5, MIN, 10 * MIN);
        for (int i = 0; i < 4; i++) {
            m.record("h", 503);
        }
        now.addAndGet(MIN);
        assertFalse(m.record("h", 503));
        assertEquals(5, m.getTotal());
        assertEquals(0, m.getTrips());
    }

    @Test
    public void testTripPausesAndDoubles() {
        AtomicLong now = new AtomicLong(1_000_000);
        RateLimitMonitor m = new RateLimitMonitor(now::get, 5, MIN, 10 * MIN);
        for (int i = 0; i < 4; i++) {
            assertFalse(m.record("h", 503));
        }
        assertTrue(m.record("h", 429));
        assertEquals(10 * MIN, m.pauseRemainingMs());
        assertEquals(Map.of(429, 1L, 503, 4L), m.getByStatus());

        // in-flight failures during the pause count but don't re-trip
        for (int i = 0; i < 10; i++) {
            assertFalse(m.record("h", 503));
        }
        assertEquals(1, m.getTrips());
        assertEquals(15, m.getTotal());

        now.addAndGet(10 * MIN);
        assertEquals(0, m.pauseRemainingMs());
        // the window was cleared on the trip, so it takes a fresh threshold to trip again
        assertTrue(m.record("h", 503, 5));
        assertEquals(20 * MIN, m.pauseRemainingMs());
        assertEquals(2, m.getTrips());
    }

    @Test
    public void testPauseIsCapped() {
        AtomicLong now = new AtomicLong(0);
        RateLimitMonitor m = new RateLimitMonitor(now::get, 1, MIN, RateLimitMonitor.MAX_PAUSE_MS);
        assertTrue(m.record("h", 503));
        now.addAndGet(RateLimitMonitor.MAX_PAUSE_MS);
        assertTrue(m.record("h", 503));
        assertEquals(RateLimitMonitor.MAX_PAUSE_MS, m.pauseRemainingMs());
    }

    @Test
    public void testBanner() {
        assertEquals(
                "Common Crawl is rate-limiting this host (503 SlowDown): 7 in 60 s. Stop and wait "
                        + "(hours), then resume at a lower request rate (DuckDB threads=1, Fetch "
                        + "numThreads=1).",
                RateLimitMonitor.banner(7, MIN));
    }

    @Test
    public void testStatusParsing() {
        assertEquals(
                503,
                BackoffHttpFetcher.statusOf(new IOException("bad status code: 503 :: SlowDown")));
        assertEquals(-1, BackoffHttpFetcher.statusOf(new IOException("connection reset")));
        assertEquals(-1, BackoffHttpFetcher.statusOf(new IOException()));
        assertEquals(
                "data.commoncrawl.org",
                BackoffHttpFetcher.hostOf("https://data.commoncrawl.org/x"));
    }

    @Test
    public void testFetcherCountsAndPausesOnLoopback() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                ex -> {
                    byte[] b = "ok".getBytes(StandardCharsets.US_ASCII);
                    int status = hits.incrementAndGet() <= 3 ? 503 : 200;
                    ex.sendResponseHeaders(status, b.length);
                    ex.getResponseBody().write(b);
                    ex.close();
                });
        server.start();
        try {
            RateLimitMonitor m = new RateLimitMonitor(System::currentTimeMillis, 2, MIN, 50);
            BackoffHttpFetcher f = TikaPipes.newBackoffHttpFetcher(new long[] {0, 0, 0, 0, 0}, m);
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/x";
            try (InputStream is = f.fetch(url, new Metadata(), new ParseContext())) {
                assertEquals("ok", new String(is.readAllBytes(), StandardCharsets.US_ASCII));
            }
            assertEquals(3, m.getTotal());
            assertEquals(1, m.getTrips());
            assertEquals(4, hits.get());
        } finally {
            server.stop(0);
        }
    }
}
