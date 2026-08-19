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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * This is a lighter class that doesn't rely on a database to extract files from CC and write a list
 * of truncated urls.
 */
public class CCFetcherCli {

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
            printHelp();
            return;
        }

        String command = args[0];

        if (command.equals("Fetch")) {
            RunLabel.primeFromConfigFile(args[1]);
            CCFileExtractor.main(new String[] {args[1]});
        } else if (command.equals("QueryIndex")) {
            RunLabel.primeFromConfigFile(args[1]);
            CCColumnarIndexExtractor.main(new String[] {args[1]});
        } else if (command.equals("FetchIndices")) {
            CCIndexFetcher.main(new String[] {args[1]});
        } else if (command.equals("CountMimes")) {
            CCMimeCounter.main(new String[] {args[1]});
        } else if (command.equals("ListCrawls")) {
            CCListCrawls.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (command.equals("QuickFetch")) {
            CCQuickFetch.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (Files.isRegularFile(Paths.get(command))) {
            RunLabel.primeFromConfigFile(args[0]);
            CCFileExtractor.main(new String[] {args[0]});
        } else {
            System.err.println("Unknown command: " + command);
            System.err.println();
            printHelp();
            System.exit(1);
        }
    }

    private static void printHelp() {
        System.out.println("commoncrawl-fetcher-lite - Extract files from Common Crawl");
        System.out.println();
        System.out.println("Usage:");
        System.out.println("  java -jar commoncrawl-fetcher-lite.jar <command> [args]");
        System.out.println("  java -jar commoncrawl-fetcher-lite.jar <config.json>");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  Fetch <config.json>       Extract files from Common Crawl (scans the CDX index)");
        System.out.println("  QueryIndex <config.json>  Extract files via the columnar (Parquet) index instead");
        System.out.println("                            of scanning CDX shards -- faster, needs AWS credentials");
        System.out.println("                            for listing (not billed; not Athena). See");
        System.out.println("                            docs/columnar-index.adoc.");
        System.out.println("  QuickFetch [options]      Fetch files without writing a config file");
        System.out.println("  FetchIndices <config.json> Download index files locally");
        System.out.println("  CountMimes <config.json>  Count MIME types in index records");
        System.out.println("  ListCrawls                List available Common Crawl crawls");
        System.out.println("  --help                    Show this help message");
        System.out.println();
        System.out.println("If the first argument is a file path, it is treated as 'Fetch <config.json>'.");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar commoncrawl-fetcher-lite.jar ListCrawls");
        System.out.println("  java -jar commoncrawl-fetcher-lite.jar my-config.json");
        System.out.println("  java -jar commoncrawl-fetcher-lite.jar QuickFetch \\");
        System.out.println("      --mime application/pdf --output ~/data/pdfs");
        System.out.println("  java -jar commoncrawl-fetcher-lite.jar QuickFetch \\");
        System.out.println("      --extension xlsx,xlsm --output ~/data/excel");
        System.out.println();
        System.out.println("For QuickFetch options, run: QuickFetch --help");
        System.out.println("See https://github.com/tballison/commoncrawl-fetcher-lite for documentation.");
    }
}
