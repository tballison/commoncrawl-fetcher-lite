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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class RateLimitPreflightTest {

    private static RateLimitMonitor monitor() {
        return new RateLimitMonitor(() -> 0L, 5, 60_000, 600_000);
    }

    private static RateLimitPreflight.StatusProbe probe(int data, int s3) {
        return uri -> uri.getHost().equals("data.commoncrawl.org") ? data : s3;
    }

    @Test
    public void testProbeUris() {
        List<URI> uris = RateLimitPreflight.probeUris("CC-MAIN-2026-43");
        assertEquals(
                "https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-43/cc-index-table.paths.gz",
                uris.get(0).toString());
        assertEquals(
                "https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2026-43/"
                        + "cc-index-table.paths.gz",
                uris.get(1).toString());
        assertTrue(
                RateLimitPreflight.probeUris("CC-MAIN-2025-*")
                        .get(0)
                        .toString()
                        .contains(RateLimitPreflight.FALLBACK_CRAWL));
    }

    @Test
    public void testOkProceeds() throws Exception {
        RateLimitPreflight.check("CC-MAIN-2026-43", probe(200, 200), monitor());
    }

    @Test
    public void testNonRateLimitErrorsProceed() throws Exception {
        RateLimitMonitor m = monitor();
        RateLimitPreflight.check("CC-MAIN-2026-43", probe(404, 403), m);
        RateLimitPreflight.check(
                "CC-MAIN-2026-43",
                uri -> {
                    throw new IOException("unreachable");
                },
                m);
        assertEquals(0, m.getTotal());
    }

    @Test
    public void testRateLimitedFailsFast() {
        for (int[] statuses : new int[][] {{503, 200}, {200, 503}, {429, 200}, {503, 503}}) {
            RateLimitMonitor m = monitor();
            IOException e =
                    assertThrows(
                            IOException.class,
                            () ->
                                    RateLimitPreflight.check(
                                            "CC-MAIN-2026-43",
                                            probe(statuses[0], statuses[1]),
                                            m));
            assertTrue(e.getMessage().startsWith("Common Crawl is rate-limiting this host"));
            assertTrue(e.getMessage().contains("-Dcc.preflight=false"), e.getMessage());
            assertTrue(m.getTotal() > 0);
        }
    }

    @Test
    public void testDecide() {
        URI a = URI.create("https://data.commoncrawl.org/x");
        URI b = URI.create("https://commoncrawl.s3.amazonaws.com/x");
        assertNull(RateLimitPreflight.decide(Map.of(a, 200, b, -1)));
        String msg = RateLimitPreflight.decide(Map.of(a, 200, b, 503));
        assertTrue(msg.contains("1 in 60 s"), msg);
        assertTrue(msg.contains("commoncrawl.s3.amazonaws.com -> HTTP 503"), msg);
    }

    @Test
    public void testDisabledSkipsProbe() throws Exception {
        String old = System.getProperty("cc.preflight");
        System.setProperty("cc.preflight", "false");
        try {
            RateLimitPreflight.check(
                    "CC-MAIN-2026-43",
                    uri -> {
                        throw new AssertionError("probed while disabled");
                    },
                    monitor());
        } finally {
            if (old == null) {
                System.clearProperty("cc.preflight");
            } else {
                System.setProperty("cc.preflight", old);
            }
        }
    }
}
