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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.api.fetcher.RangeFetcher;
import org.apache.tika.plugins.ExtensionConfig;

public class BackoffS3FetcherTest {

    private static void noSleep(long ms) {
        // the ladder's sleeps are not under test
    }

    private static S3Exception s3Exception(String message, int status, String errorCode) {
        return (S3Exception)
                S3Exception.builder()
                        .message(message)
                        .statusCode(status)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
                        .build();
    }

    private static S3Exception slowDown() {
        return s3Exception("Please reduce your request rate.", 503, "SlowDown");
    }

    /** A fake S3 fetcher: every call runs {@code body}. */
    private static RangeFetcher fake(BackoffS3Fetcher.Attempt body) {
        return new RangeFetcher() {
            @Override
            public ExtensionConfig getExtensionConfig() {
                return null;
            }

            @Override
            public TikaInputStream fetch(String k, Metadata m, ParseContext c)
                    throws TikaException, IOException {
                return body.fetch();
            }

            @Override
            public TikaInputStream fetch(String k, long s, long e, Metadata m, ParseContext c)
                    throws TikaException, IOException {
                return body.fetch();
            }
        };
    }

    @Test
    public void testRateLimitStatus() {
        // Tika's S3Fetcher wraps the SDK exception in an IOException or TikaException
        assertEquals(503, BackoffS3Fetcher.rateLimitStatus(new IOException(slowDown())));
        assertEquals(
                503, BackoffS3Fetcher.rateLimitStatus(new TikaException("client", slowDown())));
        assertEquals(
                429,
                BackoffS3Fetcher.rateLimitStatus(new IOException(s3Exception("slow", 429, null))));
        S3Exception missing = s3Exception("no such key", 404, "NoSuchKey");
        assertEquals(-1, BackoffS3Fetcher.rateLimitStatus(new IOException(missing)));
        assertEquals(-1, BackoffS3Fetcher.rateLimitStatus(new IOException("reset")));
    }

    @Test
    public void testNonRateLimitErrorIsNotRetried() {
        RateLimitMonitor m = new RateLimitMonitor(System::currentTimeMillis, 3, 60_000, 1_000);
        AtomicInteger calls = new AtomicInteger();
        BackoffS3Fetcher f =
                new BackoffS3Fetcher(
                        fake(
                                () -> {
                                    calls.incrementAndGet();
                                    throw new IOException("reset");
                                }),
                        new long[] {0, 0, 0},
                        m,
                        BackoffS3FetcherTest::noSleep);
        assertThrows(IOException.class, () -> f.fetch("k", 0, 10, new Metadata(), null));
        assertEquals(1, calls.get());
        assertEquals(0, m.getTotal());
    }

    @Test
    public void testThresholdPausesOtherThreads() throws Exception {
        long pauseMs = 1_500;
        RateLimitMonitor m = new RateLimitMonitor(System::currentTimeMillis, 3, 60_000, pauseMs);
        BackoffS3Fetcher throttled =
                new BackoffS3Fetcher(
                        fake(
                                () -> {
                                    throw new IOException(slowDown());
                                }),
                        new long[] {0, 0, 0, 0, 0},
                        m,
                        BackoffS3FetcherTest::noSleep);
        AtomicLong healthyCalledAt = new AtomicLong();
        BackoffS3Fetcher healthy =
                new BackoffS3Fetcher(
                        fake(
                                () -> {
                                    healthyCalledAt.set(System.currentTimeMillis());
                                    return TikaInputStream.get(new byte[] {1});
                                }),
                        new long[] {0},
                        m,
                        BackoffS3FetcherTest::noSleep);

        Thread a =
                new Thread(
                        () ->
                                assertThrows(
                                        ThrottleException.class,
                                        () -> throttled.fetch("a", 0, 1, new Metadata(), null)));
        a.start();
        long deadline = System.currentTimeMillis() + 5_000;
        while (m.getTrips() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(1, m.getTrips(), "three 503s within the window must trip the pause");
        long trippedAt = System.currentTimeMillis();
        long remaining = m.pauseRemainingMs();
        assertTrue(remaining > pauseMs / 2, "pause in effect: " + remaining);

        // a different thread, whose own requests never failed, must wait out the pause
        Thread b =
                new Thread(
                        () -> {
                            try (InputStream is = healthy.fetch("b", 0, 1, new Metadata(), null)) {
                                assertEquals(1, is.read());
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        b.start();
        b.join(10_000);
        a.join(10_000);
        long waited = healthyCalledAt.get() - trippedAt;
        assertTrue(waited >= remaining - 100, "other thread waited " + waited + " ms");
        assertEquals("{s3://commoncrawl=" + m.getTotal() + "}", m.getByHost().toString());
    }
}
