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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Counts rate-limit responses (503 SlowDown, 429) from Common Crawl. Every one is logged at WARN;
 * {@code threshold} within {@code windowMs} logs an ERROR banner and starts a process-wide pause
 * (10 min, doubling on each later trip) that {@link #awaitClearance()} enforces.
 */
public class RateLimitMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitMonitor.class);

    public static final int DEFAULT_THRESHOLD = 5;
    public static final long DEFAULT_WINDOW_MS = TimeUnit.SECONDS.toMillis(60);
    public static final long DEFAULT_INITIAL_PAUSE_MS = TimeUnit.MINUTES.toMillis(10);
    static final long MAX_PAUSE_MS = TimeUnit.HOURS.toMillis(24);

    private static final RateLimitMonitor GLOBAL =
            new RateLimitMonitor(
                    System::currentTimeMillis,
                    DEFAULT_THRESHOLD,
                    DEFAULT_WINDOW_MS,
                    DEFAULT_INITIAL_PAUSE_MS);

    private final LongSupplier clock;
    private final int threshold;
    private final long windowMs;
    private final ArrayDeque<Long> window = new ArrayDeque<>();
    private final Map<Integer, Long> byStatus = new TreeMap<>();
    private final Map<String, Long> byHost = new TreeMap<>();
    private long total = 0;
    private int trips = 0;
    private long nextPauseMs;
    private long pauseUntil = 0;

    public RateLimitMonitor(LongSupplier clock, int threshold, long windowMs, long initialPauseMs) {
        this.clock = clock;
        this.threshold = threshold;
        this.windowMs = windowMs;
        this.nextPauseMs = initialPauseMs;
    }

    /** Shared by every {@link BackoffHttpFetcher} and {@link BackoffS3Fetcher} in the process. */
    public static RateLimitMonitor global() {
        return GLOBAL;
    }

    public static boolean isRateLimitStatus(int status) {
        return status == 503 || status == 429;
    }

    public static String banner(long n, long windowMs) {
        return "Common Crawl is rate-limiting this host (503 SlowDown): "
                + n
                + " in "
                + TimeUnit.MILLISECONDS.toSeconds(windowMs)
                + " s. Stop and wait (hours), then resume at a lower request rate "
                + "(DuckDB threads=1, Fetch numThreads=1).";
    }

    /** Records one rate-limit response; true if it tripped the threshold. */
    public boolean record(String host, int status) {
        return record(host, status, 1);
    }

    /** Records {@code n} rate-limit responses seen at once; true if they tripped the threshold. */
    public synchronized boolean record(String host, int status, long n) {
        if (n <= 0) {
            return false;
        }
        long now = clock.getAsLong();
        total += n;
        byStatus.merge(status, n, Long::sum);
        byHost.merge(host, n, Long::sum);
        for (long i = 0; i < n; i++) {
            window.addLast(now);
        }
        while (!window.isEmpty() && window.peekFirst() <= now - windowMs) {
            window.removeFirst();
        }
        LOGGER.warn(
                "rate-limited by {}: HTTP {}{} ({} this run, {} in the last {} s)",
                host,
                status,
                n > 1 ? " x" + n : "",
                total,
                window.size(),
                TimeUnit.MILLISECONDS.toSeconds(windowMs));
        // In-flight requests keep failing during a pause; they must not re-trip and double it.
        if (now < pauseUntil || window.size() < threshold) {
            return false;
        }
        trips++;
        long pauseMs = nextPauseMs;
        nextPauseMs = Math.min(nextPauseMs * 2, MAX_PAUSE_MS);
        pauseUntil = now + pauseMs;
        LOGGER.error(banner(window.size(), windowMs));
        LOGGER.error(
                "pausing all Common Crawl requests (HTTP and S3, every fetch thread) for {} min "
                        + "(pause #{}; the next one doubles)",
                TimeUnit.MILLISECONDS.toMinutes(pauseMs),
                trips);
        window.clear();
        return true;
    }

    public synchronized long pauseRemainingMs() {
        return Math.max(0, pauseUntil - clock.getAsLong());
    }

    /** Blocks while a rate-limit pause is in effect. */
    public void awaitClearance() throws InterruptedException {
        long remaining;
        while ((remaining = pauseRemainingMs()) > 0) {
            Thread.sleep(Math.min(remaining, TimeUnit.MINUTES.toMillis(1)));
        }
    }

    public synchronized long getTotal() {
        return total;
    }

    public synchronized int getTrips() {
        return trips;
    }

    public synchronized Map<Integer, Long> getByStatus() {
        return new TreeMap<>(byStatus);
    }

    public synchronized Map<String, Long> getByHost() {
        return new TreeMap<>(byHost);
    }

    @Override
    public synchronized String toString() {
        return "rate-limited responses: " + total + " " + byStatus + " " + byHost + ", pauses: "
                + trips;
    }
}
