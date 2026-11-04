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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tallison.cc.index.extractor.ExtractorConfig;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;

/**
 * We need this because it allows for back-off on 503/429 (counted and escalated to a hard pause by
 * {@link RateLimitMonitor}) and it adds the protocol+host base to the
 * paths: {@link ExtractorConfig#CC_HTTPS_BASE}
 */
public class BackoffHttpFetcher extends HttpFetcher {

    private static Logger LOGGER = LoggerFactory.getLogger(BackoffHttpFetcher.class);
    private static final Pattern BAD_STATUS = Pattern.compile("bad status code: (\\d+)");

    // backoff
    private final long[] throttleSeconds;
    private final RateLimitMonitor monitor;

    public BackoffHttpFetcher(long[] throttleSeconds) {
        this(throttleSeconds, RateLimitMonitor.global());
    }

    BackoffHttpFetcher(long[] throttleSeconds, RateLimitMonitor monitor) {
        this.throttleSeconds = throttleSeconds;
        this.monitor = monitor;
    }

    @Override
    public InputStream fetch(String fetchKey, Metadata metadata, ParseContext parseContext)
            throws TikaException, IOException {
        return fetchWithBackOff(new FetchKey("name", getUrl(fetchKey)), metadata, parseContext);
    }

    @Override
    public InputStream fetch(String fetchkey, long rangeStart, long rangeEnd, Metadata metadata)
            throws IOException {
        return fetchWithBackOff(
                new FetchKey("name", getUrl(fetchkey), rangeStart, rangeEnd),
                metadata,
                new ParseContext());
    }

    private String getUrl(String fetchKey) {
        if (!fetchKey.startsWith("http")) {
            if (fetchKey.startsWith("/")) {
                return ExtractorConfig.CC_HTTPS_BASE + fetchKey;
            } else {
                return ExtractorConfig.CC_HTTPS_BASE + "/" + fetchKey;
            }
        }
        return fetchKey;
    }

    private InputStream fetchWithBackOff(
            FetchKey fetchKey, Metadata metadata, ParseContext parseContext) throws IOException {
        int tries = 0;
        while (tries < throttleSeconds.length) {
            try {
                monitor.awaitClearance();
                return _fetch(fetchKey, metadata);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted during rate-limit pause", e);
            } catch (IOException e) {
                int status = statusOf(e);
                boolean isThrottle =
                        RateLimitMonitor.isRateLimitStatus(status)
                                || (status == 403 && e.getMessage().contains("too much traffic"));
                if (!isThrottle) {
                    throw e;
                }
                monitor.record(hostOf(fetchKey.getFetchKey()), status);
                if (monitor.pauseRemainingMs() > 0) {
                    // the hard pause replaces the ladder step; awaitClearance() waits it out
                    tries++;
                    continue;
                }
                LOGGER.warn(
                        "got backoff warning (#{}) for {}. Will sleep {} seconds. Message: {}. ",
                        tries + 1,
                        fetchKey.getFetchKey(),
                        throttleSeconds[tries],
                        e.getMessage());
                try {
                    Thread.sleep(1000 * throttleSeconds[tries]);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted during backoff", ex);
                }
            } catch (TikaException e) {
                throw new IOException(e);
            }
            tries++;
        }
        throw new ThrottleException();
    }

    /** HTTP status from HttpFetcher's "bad status code: NNN" message, or -1. */
    static int statusOf(IOException e) {
        if (e.getMessage() == null) {
            return -1;
        }
        Matcher m = BAD_STATUS.matcher(e.getMessage());
        return m.find() ? Integer.parseInt(m.group(1)) : -1;
    }

    static String hostOf(String url) {
        try {
            String host = URI.create(url).getHost();
            return host == null ? url : host;
        } catch (IllegalArgumentException e) {
            return url;
        }
    }

    private TikaInputStream _fetch(FetchKey fetchKey, Metadata metadata)
            throws IOException, TikaException {
        if (fetchKey.getRangeStart() > 0) {
            return (TikaInputStream)
                    super.fetch(
                            fetchKey.getFetchKey(),
                            fetchKey.getRangeStart(),
                            fetchKey.getRangeEnd(),
                            metadata);
        } else {
            return (TikaInputStream)
                    super.fetch(fetchKey.getFetchKey(), metadata, new ParseContext());
        }
    }

    @Override
    public String getName() {
        return "backoffHttpFetcher";
    }
}
