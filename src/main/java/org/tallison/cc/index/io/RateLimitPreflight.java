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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.tallison.cc.index.extractor.ExtractorConfig;

/**
 * Before a DuckDB scan, HEADs one small object on data.commoncrawl.org and on S3 and fails fast on
 * 503/429: DuckDB's own retries would otherwise turn per-IP rate limiting into hours of slow
 * reads. {@code -Dcc.preflight=false} skips it.
 */
public class RateLimitPreflight {

    private static final Logger LOGGER = LoggerFactory.getLogger(RateLimitPreflight.class);

    // used when crawlGlob is a wildcard; any small, stable object will do
    static final String FALLBACK_CRAWL = "CC-MAIN-2024-10";

    /** Returns the HTTP status for a HEAD of the URI. */
    public interface StatusProbe {
        int status(URI uri) throws IOException, InterruptedException;
    }

    public static boolean isEnabled() {
        return !"false".equalsIgnoreCase(System.getProperty("cc.preflight", "true"));
    }

    static List<URI> probeUris(String crawlGlob) {
        String crawl = crawlGlob.matches("CC-MAIN-\\d{4}-\\d{2}") ? crawlGlob : FALLBACK_CRAWL;
        String key = "crawl-data/" + crawl + "/cc-index-table.paths.gz";
        return List.of(
                URI.create(ExtractorConfig.CC_HTTPS_BASE + "/" + key),
                URI.create("https://" + ExtractorConfig.CC_S3_BUCKET + ".s3.amazonaws.com/" + key));
    }

    /**
     * Probes each URI; returns statuses (-1 where the probe itself failed). Rate-limit statuses
     * are recorded in {@code monitor}.
     */
    static Map<URI, Integer> probe(List<URI> uris, StatusProbe probe, RateLimitMonitor monitor)
            throws InterruptedException {
        Map<URI, Integer> statuses = new LinkedHashMap<>();
        for (URI uri : uris) {
            int status;
            try {
                status = probe.status(uri);
            } catch (IOException e) {
                LOGGER.warn("preflight: couldn't reach {}: {}", uri, e.toString());
                status = -1;
            }
            statuses.put(uri, status);
            if (RateLimitMonitor.isRateLimitStatus(status)) {
                monitor.record(uri.getHost(), status);
            } else if (status != 200 && status != -1) {
                LOGGER.warn("preflight: unexpected HTTP {} from {}", status, uri);
            }
        }
        return statuses;
    }

    /** Null if the scan may proceed, else the reason to stop. */
    static String decide(Map<URI, Integer> statuses) {
        StringBuilder limited = new StringBuilder();
        for (Map.Entry<URI, Integer> e : statuses.entrySet()) {
            if (RateLimitMonitor.isRateLimitStatus(e.getValue())) {
                limited.append(limited.length() == 0 ? "" : ", ")
                        .append(e.getKey().getHost())
                        .append(" -> HTTP ")
                        .append(e.getValue());
            }
        }
        if (limited.length() == 0) {
            return null;
        }
        return RateLimitMonitor.banner(
                        statuses.values().stream()
                                .filter(RateLimitMonitor::isRateLimitStatus)
                                .count(),
                        RateLimitMonitor.DEFAULT_WINDOW_MS)
                + " Preflight: "
                + limited
                + ". Override with -Dcc.preflight=false.";
    }

    /** Runs the preflight for a columnar scan; throws if Common Crawl is rate-limiting us. */
    public static void check(String crawlGlob) throws IOException, InterruptedException {
        check(crawlGlob, RateLimitPreflight::head, RateLimitMonitor.global());
    }

    static void check(String crawlGlob, StatusProbe probe, RateLimitMonitor monitor)
            throws IOException, InterruptedException {
        if (!isEnabled()) {
            LOGGER.info("preflight disabled (-Dcc.preflight=false)");
            return;
        }
        Map<URI, Integer> statuses = probe(probeUris(crawlGlob), probe, monitor);
        LOGGER.info("preflight: {}", statuses);
        String failure = decide(statuses);
        if (failure != null) {
            LOGGER.error(failure);
            throw new IOException(failure);
        }
    }

    private static int head(URI uri) throws IOException, InterruptedException {
        HttpClient client =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(20))
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
        HttpRequest req =
                HttpRequest.newBuilder(uri)
                        .method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .timeout(Duration.ofSeconds(30))
                        .build();
        return client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
    }
}
