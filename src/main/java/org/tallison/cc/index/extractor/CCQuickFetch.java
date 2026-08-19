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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CLI shorthand for common fetch operations without writing a JSON config file.
 *
 * <p>Usage examples:
 * <pre>
 *   QuickFetch --mime application/pdf --output ~/data/pdfs
 *   QuickFetch --mime application/pdf --crawl CC-MAIN-2025-51 --max 1000
 *   QuickFetch --extension xlsx,xlsm --output ~/data/excel --crawl latest
 *   QuickFetch --extension pdf,docx --dry-run
 * </pre>
 */
public class CCQuickFetch {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        List<String> mimeTypes = new ArrayList<>();
        List<String> extensions = new ArrayList<>();
        String crawl = "latest";
        String output = "docs";
        long maxFiles = -1;
        int threads = 2;
        boolean dryRun = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mime":
                    i++;
                    for (String m : args[i].split(",")) {
                        mimeTypes.add(m.trim());
                    }
                    break;
                case "--extension":
                    i++;
                    for (String e : args[i].split(",")) {
                        extensions.add(e.trim());
                    }
                    break;
                case "--crawl":
                    crawl = args[++i];
                    break;
                case "--output":
                    output = args[++i];
                    break;
                case "--max":
                    maxFiles = Long.parseLong(args[++i]);
                    break;
                case "--threads":
                    threads = Integer.parseInt(args[++i]);
                    break;
                case "--dry-run":
                    dryRun = true;
                    break;
                case "--help":
                    printUsage();
                    return;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        if (mimeTypes.isEmpty() && extensions.isEmpty()) {
            System.err.println("Error: must specify at least one --mime or --extension");
            printUsage();
            System.exit(1);
        }

        // Resolve "latest" crawl
        String crawlId = resolveCrawl(crawl);

        // Build config JSON
        String configJson = buildConfigJson(
                mimeTypes, extensions, crawlId, output, maxFiles, threads, dryRun);

        System.out.println("Generated config:");
        System.out.println(configJson);
        System.out.println();

        // Write to temp file and run
        File tempConfig = File.createTempFile("cc-quick-fetch-", ".json");
        tempConfig.deleteOnExit();
        new ObjectMapper().readTree(configJson); // validate JSON
        java.nio.file.Files.writeString(tempConfig.toPath(), configJson);

        System.out.println("Starting fetch from " + crawlId + "...");
        System.out.println();

        RunLabel.primeFromConfigFile(tempConfig.getAbsolutePath());
        CCFileExtractor.main(new String[]{tempConfig.getAbsolutePath()});
    }

    private static String resolveCrawl(String crawl) {
        if ("latest".equalsIgnoreCase(crawl)) {
            System.out.println("Resolving latest crawl...");
            String latest = CCListCrawls.getLatestCrawlId();
            if (latest == null) {
                System.err.println(
                        "Could not determine latest crawl. "
                        + "Specify a crawl ID explicitly with --crawl CC-MAIN-YYYY-WW");
                System.exit(1);
            }
            System.out.println("Using latest crawl: " + latest);
            return latest;
        }
        return crawl;
    }

    static String buildConfigJson(
            List<String> mimeTypes, List<String> extensions,
            String crawlId, String output,
            long maxFiles, int threads, boolean dryRun) throws IOException {

        // Build the config as a map and serialize with Jackson for proper escaping
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> config = new LinkedHashMap<>();

        config.put("dryRun", dryRun);

        Map<String, Object> fetcher = new LinkedHashMap<>();
        fetcher.put("throttleSeconds", new long[]{30, 60, 120, 240, 480, 600, 900, 1800});
        config.put("fetcher", fetcher);

        Map<String, Object> indices = new LinkedHashMap<>();
        indices.put("paths", List.of(
                "crawl-data/" + crawlId + "/cc-index.paths.gz"));
        config.put("indices", indices);

        Map<String, Object> docs = new LinkedHashMap<>();
        docs.put("path", output);
        config.put("docs", docs);

        if (maxFiles > 0) {
            config.put("maxFilesExtracted", maxFiles);
        } else {
            config.put("maxFilesExtracted", -1);
        }
        config.put("maxFilesTruncated", -1);
        config.put("maxRecords", -1);
        config.put("numThreads", threads);

        // Build recordSelector
        Map<String, Object> recordSelector = new LinkedHashMap<>();

        Map<String, Object> must = new LinkedHashMap<>();
        must.put("status", List.of(Map.of("match", "200")));
        recordSelector.put("must", must);

        Map<String, Object> should = new LinkedHashMap<>();

        if (!mimeTypes.isEmpty()) {
            List<Map<String, String>> mimeMatches = new ArrayList<>();
            for (String mime : mimeTypes) {
                mimeMatches.add(Map.of("match", mime));
            }
            should.put("mime_detected", mimeMatches);
        }

        if (!extensions.isEmpty()) {
            should.put("url",
                    List.of(Map.of("extensions", String.join(",", extensions))));
        }

        recordSelector.put("should", should);
        config.put("recordSelector", recordSelector);
        config.put("targetPathPattern", "xx/xx/xxx");

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    }

    private static void printUsage() {
        System.out.println("QuickFetch - fetch files from Common Crawl without writing a config file");
        System.out.println();
        System.out.println("Usage: QuickFetch [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --mime <type>        MIME type(s) to fetch (comma-separated)");
        System.out.println("  --extension <ext>    File extension(s) to match (comma-separated)");
        System.out.println("  --crawl <id>         Crawl ID (default: latest)");
        System.out.println("                       e.g. CC-MAIN-2025-51 or 'latest'");
        System.out.println("  --output <path>      Output directory (default: docs)");
        System.out.println("  --max <n>            Max files to extract (default: unlimited)");
        System.out.println("  --threads <n>        Number of worker threads (default: 2)");
        System.out.println("  --dry-run            Scan index only, don't download files");
        System.out.println("  --help               Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  QuickFetch --mime application/pdf --output ~/data/pdfs");
        System.out.println("  QuickFetch --mime application/pdf --crawl CC-MAIN-2025-51 --max 1000");
        System.out.println("  QuickFetch --extension xlsx,xlsm --output ~/data/excel");
        System.out.println("  QuickFetch --mime application/vnd.ms-outlook --dry-run");
        System.out.println();
        System.out.println("Common MIME types:");
        System.out.println("  application/pdf");
        System.out.println("  application/vnd.openxmlformats-officedocument.wordprocessingml.document  (docx)");
        System.out.println("  application/vnd.openxmlformats-officedocument.spreadsheetml.sheet        (xlsx)");
        System.out.println("  application/vnd.openxmlformats-officedocument.presentationml.presentation (pptx)");
        System.out.println("  application/vnd.ms-excel.sheet.macroEnabled.12                           (xlsm)");
        System.out.println("  application/vnd.ms-word.document.macroEnabled.12                         (docm)");
        System.out.println("  application/vnd.ms-powerpoint.presentation.macroEnabled.12               (pptm)");
        System.out.println("  application/msword                                                       (doc)");
        System.out.println("  application/vnd.ms-excel                                                 (xls)");
        System.out.println("  application/vnd.ms-powerpoint                                            (ppt)");
        System.out.println("  application/epub+zip                                                     (epub)");
        System.out.println("  application/vnd.ms-outlook                                               (msg)");
        System.out.println("  image/jpeg, image/png, image/gif, image/webp, image/tiff");
        System.out.println("  video/mp4, video/webm, video/quicktime");
        System.out.println("  audio/mpeg, audio/wav, audio/ogg");
    }
}
