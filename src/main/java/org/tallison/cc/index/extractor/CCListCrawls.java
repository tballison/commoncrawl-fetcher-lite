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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Lists available Common Crawl crawls by fetching the collinfo.json endpoint.
 */
public class CCListCrawls {

    private static final String COLLINFO_URL =
            "https://index.commoncrawl.org/collinfo.json";

    private static final Pattern CRAWL_ID_PATTERN =
            Pattern.compile("CC-MAIN-\\d{4}-\\d{2}");

    public static void main(String[] args) throws Exception {
        List<String> crawls = fetchCrawlIds();
        if (crawls.isEmpty()) {
            System.err.println("Could not retrieve crawl list from " + COLLINFO_URL);
            System.exit(1);
        }
        System.out.println("Available Common Crawl crawls (most recent first):");
        System.out.println();
        for (int i = 0; i < crawls.size(); i++) {
            String id = crawls.get(i);
            String label = (i == 0) ? id + "  <-- latest" : id;
            System.out.println("  " + label);
        }
        System.out.println();
        System.out.println("Total: " + crawls.size() + " crawls");
        System.out.println();
        System.out.println("To use in a config file:");
        System.out.println("  \"indices\": { \"paths\": "
                + "[\"crawl-data/" + crawls.get(0) + "/cc-index.paths.gz\"] }");
    }

    /**
     * Fetches the list of crawl IDs from the Common Crawl collinfo.json endpoint.
     * Returns crawl IDs sorted most-recent-first.
     */
    public static List<String> fetchCrawlIds() throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(COLLINFO_URL))
                .GET()
                .build();
        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException(
                    "Failed to fetch crawl list: HTTP " + response.statusCode());
        }

        // Parse crawl IDs from the JSON response.
        // The response is a JSON array of objects with an "id" field like
        // "CC-MAIN-2025-51". We use a simple regex to avoid adding a JSON
        // dependency for this one use case (Jackson is available but this
        // keeps the class self-contained).
        List<String> crawlIds = new ArrayList<>();
        Matcher m = CRAWL_ID_PATTERN.matcher(response.body());
        while (m.find()) {
            String id = m.group();
            if (!crawlIds.contains(id)) {
                crawlIds.add(id);
            }
        }
        // The collinfo.json endpoint returns crawls in reverse chronological order,
        // so the list is already most-recent-first. Sort defensively.
        Collections.sort(crawlIds, Collections.reverseOrder());
        return crawlIds;
    }

    /**
     * Returns the most recent crawl ID, or null if the list cannot be fetched.
     */
    public static String getLatestCrawlId() {
        try {
            List<String> crawls = fetchCrawlIds();
            return crawls.isEmpty() ? null : crawls.get(0);
        } catch (IOException | InterruptedException e) {
            return null;
        }
    }
}
