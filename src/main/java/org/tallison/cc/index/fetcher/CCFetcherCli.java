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
package org.tallison.cc.index.fetcher;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.AbstractRecordProcessor;
import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.io.HTTPFetchWrapper;

/**
 * This is a lighter class that doesn't rely on a database
 * to extract files from CC and write a list of truncated urls.
 * <p>
 * Support only for http, not s3.
 */
public class CCFetcherCli {

    private static final String STOP_SEMAPHORE = StringUtils.EMPTY;
    private static final Integer INDEX_WORKER_ID = 1;
    private static final Integer INDEX_READER_ID = 2;
    private static final Integer TRUNCATED_WRITER_ID = 3;
    private static Logger LOGGER = LoggerFactory.getLogger(CCFetcherCli.class);

    public static void main(String[] args) throws Exception {
        FetcherConfig fetcherConfig =
                new ObjectMapper().readValue(new File(args[0]), FetcherConfig.class);
        execute(fetcherConfig);
    }

    private static void execute(FetcherConfig fetcherConfig) throws TikaException {
        ArrayBlockingQueue<String> indexPathsList = new ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<String> truncatedUrls = new ArrayBlockingQueue<>(1000);
        //IndexPathsReader reads a file containing a list of cc-index.paths files
        //and writes the literal gz files (cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00000.gz)
        //to indexPathsList

        //TruncatedURLWriter writes the urls that had truncated data to a text file

        //IndexWorker reads a single index.gz file at a time and processes each record
        //It fetches non truncated files, and writes truncated files to the TruncatedURLWriter
        int totalThreads = fetcherConfig.getNumThreads() + 2;

        ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
        ExecutorCompletionService<Integer> executorCompletionService =
                new ExecutorCompletionService<>(executorService);

        IndexPathsReader indexPathsReader =
                new IndexPathsReader(fetcherConfig.getIndexPathsFile(), indexPathsList);

        TruncatedURLWriter truncatedURLWriter =
                new TruncatedURLWriter(truncatedUrls, fetcherConfig.getTruncatedUrlsFile());
        executorCompletionService.submit(indexPathsReader);
        executorCompletionService.submit(truncatedURLWriter);
        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        int finishedWorkers = 0;
        try {
            for (int i = 0; i < fetcherConfig.getNumThreads(); i++) {
                FetchLiteRecordProcessor processor =
                        new FetchLiteRecordProcessor(fetcherConfig, truncatedUrls, counter);
                executorCompletionService.submit(new IndexWorker(indexPathsList, processor));
            }


            while (finishedWorkers < fetcherConfig.getNumThreads()) {
                //blocking
                Future<Integer> future = executorCompletionService.take();
                if (future != null) {
                    Integer f = future.get();
                    LOGGER.debug("completed {}", f);
                    if (f == INDEX_WORKER_ID) {
                        finishedWorkers++;
                    } else if (f == INDEX_READER_ID) {
                        LOGGER.info("Index paths reader successfully completed");
                    } else if (f == TRUNCATED_WRITER_ID) {
                        LOGGER.warn("Truncated writer finished but should not have!!!");
                    }
                }
            }
            //now tell the truncated writer to stop
            truncatedUrls.put(STOP_SEMAPHORE);
            boolean truncatedWriterWriting = true;
            while (truncatedWriterWriting) {
                Future<Integer> future = executorCompletionService.poll(60, TimeUnit.SECONDS);
                if (future != null) {
                    Integer f = future.get();
                    LOGGER.debug("completed {}", f);
                    if (f == TRUNCATED_WRITER_ID) {
                        truncatedWriterWriting = false;
                        LOGGER.info("truncated writer successfully completed");
                    }
                }
            }
        } catch (TikaConfigException | IOException e) {
            LOGGER.error("main loop exception", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LOGGER.error("main loop exception", e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            LOGGER.error("main loop interrupted exception", e);
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
    }

    private static class IndexWorker implements Callable<Integer> {

        private final ArrayBlockingQueue<String> indexUrls;
        private final AbstractRecordProcessor recordProcessor;

        private final HTTPFetchWrapper httpFetchWrapper;

        IndexWorker(ArrayBlockingQueue<String> indexUrls,
                    AbstractRecordProcessor recordProcessor) throws TikaException {
            this.indexUrls = indexUrls;
            this.recordProcessor = recordProcessor;
            httpFetchWrapper = new HTTPFetchWrapper();
        }

        @Override
        public Integer call() throws Exception {
            boolean shouldContinue = true;
            while (shouldContinue) {

                String indexUrl = indexUrls.poll(30, TimeUnit.MINUTES);
                if (indexUrl == null) {
                    throw new TimeoutException("waited 5 minutes for a new record");
                }

                if (indexUrl == STOP_SEMAPHORE) {
                    recordProcessor.close();
                    //can hang forever
                    indexUrls.put(STOP_SEMAPHORE);
                    return INDEX_WORKER_ID;
                }
                LOGGER.trace(indexUrl);
                shouldContinue = processFile(indexUrl, recordProcessor);
            }
            return INDEX_WORKER_ID;
        }

        private boolean processFile(String url, AbstractRecordProcessor recordProcessor) {
            url = FetcherConfig.CC_HTTPS_BASE + url;
            long start = System.currentTimeMillis();
            LOGGER.info("starting to process index gz: {}", url);
            try (TikaInputStream tis = httpFetchWrapper.openStream(url)) {
                try (InputStream is =
                             new BufferedInputStream(new GZIPInputStream(
                                     tis))) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(is, StandardCharsets.UTF_8))) {
                        String line = reader.readLine();
                        int lines = 0;
                        long elapsed = System.currentTimeMillis() - start;
                        LOGGER.info("Took {} ms to download {} bytes for the index gz: {}",
                                String.format("%,d", elapsed),
                                String.format("%,d", tis.getLength()), url);
                        while (line != null) {
                            LOGGER.trace("about to add a line");
                            if (StringUtils.isBlank(line)) {
                                line = reader.readLine();
                                continue;
                            }
                            try {
                                boolean shouldContinue = recordProcessor.process(line);
                                if (!shouldContinue) {
                                    return shouldContinue;
                                }
                            } catch (IOException e) {
                                LOGGER.warn("bad json: " + line);
                            }
                            lines++;
                            line = reader.readLine();
                        }
                    }
                }
            } catch (TikaException | IOException e) {
                LOGGER.warn("ugh", e);
                //TODO revisit this.
                throw new RuntimeException(e);
            }
            long elapsed = System.currentTimeMillis() - start;
            LOGGER.info("finished processing index gz in ({}) ms: {}",
                    String.format("%,d", elapsed), url);
            return true;
        }
    }

    private static class IndexPathsReader implements Callable<Integer> {
        //list of indexPaths files to read
        //e.g. https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-06/cc-index.paths.gz
        //https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/cc-index.paths.gz
        private final Path indexPathLists;
        //this is a list index paths without the https: or s3: host/bucket
        //e.g. cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00000.gz
        private final ArrayBlockingQueue<String> indexFiles;

        HTTPFetchWrapper httpFetchWrapper;

        private IndexPathsReader(Path indexPathLists, ArrayBlockingQueue<String> indexFiles)
                throws TikaException {
            this.indexPathLists = indexPathLists;
            this.indexFiles = indexFiles;
            httpFetchWrapper = new HTTPFetchWrapper();
        }

        @Override
        public Integer call() throws Exception {

            try (BufferedReader reader = Files.newBufferedReader(indexPathLists)) {
                String line = reader.readLine();
                while (line != null) {
                    try {
                        if (line.startsWith("#")) {
                            line = reader.readLine();
                            continue;
                        }
                        if (line.endsWith("cc-index.paths.gz")) {
                            loadIndexPaths(line);
                            //don't overwhelm aws...seriously...
                            Thread.sleep(30000);
                        } else if (line.startsWith("cc-index") && line.endsWith(".gz")) {
                            //hangs permanently
                            indexFiles.put(line);
                            LOGGER.debug("added {} to index files", line);
                        }
                    } catch (IOException e) {
                        LOGGER.warn("problem loading index path: " + line, e);
                    }
                    line = reader.readLine();
                }
            } catch (InterruptedException e) {
                LOGGER.debug("c'est la vie. index reader was interrupted");
            } finally {
                //hangs permanently
                indexFiles.put(STOP_SEMAPHORE);
            }
            return INDEX_READER_ID;
        }

        private void loadIndexPaths(String indexPathFile)
                throws TikaException, IOException, InterruptedException {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            new GZIPInputStream(
                                    httpFetchWrapper.openStream(indexPathFile))
                            , StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                while (line != null) {
                    if (line.startsWith("#")) {
                        line = reader.readLine();
                        continue;
                    }
                    if (line.endsWith(".gz")) {
                        //hangs permanently
                        indexFiles.put(line);
                        LOGGER.debug("added {} to index files", line);
                    }
                    line = reader.readLine();
                }
            }
        }
    }

    private static class TruncatedURLWriter implements Callable<Integer> {
        private final ArrayBlockingQueue<String> truncatedUrls;
        private final Path truncatedUrlFile;

        private TruncatedURLWriter(ArrayBlockingQueue<String> truncatedUrls,
                                   Path truncatedUrlFile) {
            this.truncatedUrls = truncatedUrls;
            this.truncatedUrlFile = truncatedUrlFile;
        }

        @Override
        public Integer call() throws Exception {
            try (BufferedWriter writer = Files.newBufferedWriter(truncatedUrlFile,
                    StandardCharsets.UTF_8)) {
                while (true) {
                    //blocks forever
                    String url = truncatedUrls.take();
                    if (url == STOP_SEMAPHORE) {
                        return TRUNCATED_WRITER_ID;
                    }
                    url = url.replaceAll("[\r\n]", " ");
                    writer.write(url + "\n");
                }
            }
        }
    }
}
