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

import com.amazonaws.AmazonServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.fetcher.RangeFetcher;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;

/**
 * S3 counterpart of {@link BackoffHttpFetcher}: S3 503 SlowDown / 429 go into the process-wide
 * {@link RateLimitMonitor}, so crossing its threshold pauses every fetch thread, and every attempt
 * waits out a pause first. The wrapped fetcher gets a single attempt per call (its own retry
 * ladder would hide the 503s); this class owns the {@code throttleSeconds} ladder.
 */
public class BackoffS3Fetcher implements RangeFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackoffS3Fetcher.class);
    static final String HOST = "s3://commoncrawl";

    interface Sleeper {
        void sleep(long ms) throws InterruptedException;
    }

    @FunctionalInterface
    interface Attempt {
        InputStream fetch() throws TikaException, IOException;
    }

    private final RangeFetcher delegate;
    private final long[] throttleSeconds;
    private final RateLimitMonitor monitor;
    private final Sleeper sleeper;

    /** {@code s3} must be configured; its own retries are turned off here. */
    public static BackoffS3Fetcher wrap(S3Fetcher s3, long[] throttleSeconds) {
        // one try; Tika indexes throttleSeconds[0] after a failure, so it can't be empty
        s3.setThrottleSeconds(new long[] {0});
        return new BackoffS3Fetcher(s3, throttleSeconds, RateLimitMonitor.global(), Thread::sleep);
    }

    BackoffS3Fetcher(
            RangeFetcher delegate,
            long[] throttleSeconds,
            RateLimitMonitor monitor,
            Sleeper sleeper) {
        this.delegate = delegate;
        this.throttleSeconds = throttleSeconds;
        this.monitor = monitor;
        this.sleeper = sleeper;
    }

    @Override
    public String getName() {
        return "backoffS3Fetcher";
    }

    @Override
    public InputStream fetch(String fetchKey, Metadata metadata, ParseContext parseContext)
            throws TikaException, IOException {
        return withBackoff(fetchKey, () -> delegate.fetch(fetchKey, metadata, parseContext));
    }

    @Override
    public InputStream fetch(
            String fetchKey,
            long rangeStart,
            long rangeEnd,
            Metadata metadata,
            ParseContext parseContext)
            throws TikaException, IOException {
        return withBackoff(
                fetchKey,
                () -> delegate.fetch(fetchKey, rangeStart, rangeEnd, metadata, parseContext));
    }

    InputStream withBackoff(String fetchKey, Attempt attempt) throws TikaException, IOException {
        for (int tries = 0; ; tries++) {
            try {
                monitor.awaitClearance();
                return attempt.fetch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted during rate-limit pause", e);
            } catch (TikaException | IOException e) {
                int status = rateLimitStatus(e);
                if (status < 0) {
                    throw e;
                }
                monitor.record(HOST, status);
                if (tries >= throttleSeconds.length) {
                    throw new ThrottleException();
                }
                if (monitor.pauseRemainingMs() > 0) {
                    // the global pause replaces this ladder step
                    continue;
                }
                LOGGER.warn(
                        "S3 {} (#{}) for {}; sleeping {} s",
                        status,
                        tries + 1,
                        fetchKey,
                        throttleSeconds[tries]);
                try {
                    sleeper.sleep(1000 * throttleSeconds[tries]);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted during backoff", ie);
                }
            }
        }
    }

    /** 503 or 429 if a rate-limit response is anywhere in the cause chain, else -1. */
    static int rateLimitStatus(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof AmazonServiceException) {
                AmazonServiceException ase = (AmazonServiceException) c;
                if ("SlowDown".equals(ase.getErrorCode())) {
                    return 503;
                }
                if (RateLimitMonitor.isRateLimitStatus(ase.getStatusCode())) {
                    return ase.getStatusCode();
                }
            }
        }
        return -1;
    }
}
