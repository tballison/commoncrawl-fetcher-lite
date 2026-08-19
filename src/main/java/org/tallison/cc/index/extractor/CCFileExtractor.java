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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;
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

import org.tallison.cc.index.AbstractRecordProcessor;
import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.IndexIterator;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.pipesiterator.CallablePipesIterator;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.apache.tika.utils.StringUtils;

/**
 * This is a lighter class that doesn't rely on a database to extract files from CC and log a list
 * of truncated urls.
 */
public class CCFileExtractor {

    private static final Long INDEX_WORKER_ID = 42L;
    private static final Long INDEX_ITERATOR_ID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CCFileExtractor.class);

    public static void main(String[] args) throws Exception {
        ExtractorConfig fetcherConfig =
                new ObjectMapper().readValue(new File(args[0]), ExtractorConfig.class);
        execute(fetcherConfig);
    }

    private static void execute(ExtractorConfig fetcherConfig) throws TikaException {
        ArrayBlockingQueue<FetchEmitTuple> indexFileQueue = new ArrayBlockingQueue<>(1000);
        // The IndexIterator resolves configured paths (which may be index lists or literal
        // index file paths) and enqueues individual index file paths (e.g. cdx-00000.gz)
        // for workers to process.

        // Each IndexWorker fetches and processes one index file (cdx-*.gz) at a time,
        // extracting non-truncated files and logging truncated URLs.
        int totalThreads = fetcherConfig.getNumThreads() + 1;

        ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
        ExecutorCompletionService<Long> executorCompletionService =
                new ExecutorCompletionService<>(executorService);

        IndexIterator indexIterator = fetcherConfig.getIndexIterator();
        indexIterator.initialize(Collections.EMPTY_MAP);
        executorCompletionService.submit(new CallablePipesIterator(indexIterator, indexFileQueue));
        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        int totalIndexFiles = indexIterator.getResolvedIndexFileCount();
        counter.setTotalIndexFiles(totalIndexFiles);
        LOGGER.info("Resolved {} index files to process", totalIndexFiles);
        int finishedWorkers = 0;
        try {
            for (int i = 0; i < fetcherConfig.getNumThreads(); i++) {
                CCFileExtractorRecordProcessor processor =
                        new CCFileExtractorRecordProcessor(fetcherConfig, counter);
                executorCompletionService.submit(
                        new IndexWorker(
                                fetcherConfig, indexFileQueue,
                                processor, counter));
            }

            while (finishedWorkers < fetcherConfig.getNumThreads()) {
                // blocking
                Future<Long> future = executorCompletionService.take();
                if (future != null) {
                    Long f = future.get();
                    LOGGER.debug("completed {}", f);
                    if (f.equals(INDEX_WORKER_ID)) {
                        finishedWorkers++;
                    } else if (f.equals(INDEX_ITERATOR_ID)) {
                        LOGGER.info("Index paths reader successfully completed");
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
            LOGGER.warn("main loop interrupted exception", e);
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
        LOGGER.info("Finished: {}", counter);
    }

    private static class IndexWorker implements Callable<Long> {

        private final ArrayBlockingQueue<FetchEmitTuple> indexUrls;
        private final AbstractRecordProcessor recordProcessor;
        private final CCIndexReaderCounter counter;

        private final Fetcher indexFetcher;

        IndexWorker(
                ExtractorConfig fetcherConfig,
                ArrayBlockingQueue<FetchEmitTuple> indexUrls,
                CCFileExtractorRecordProcessor recordProcessor,
                CCIndexReaderCounter counter)
                throws TikaException {
            this.indexUrls = indexUrls;
            this.recordProcessor = recordProcessor;
            this.counter = counter;
            this.indexFetcher = fetcherConfig.newIndexFileFetcher();
        }

        @Override
        public Long call() throws Exception {
            boolean shouldContinue = true;
            while (shouldContinue) {

                FetchEmitTuple indexUrl = indexUrls.poll(120, TimeUnit.MINUTES);
                if (indexUrl == null) {
                    throw new TimeoutException("waited 120 minutes for a new record");
                }

                if (indexUrl == PipesIterator.COMPLETED_SEMAPHORE) {
                    recordProcessor.close();
                    // can hang forever
                    indexUrls.put(PipesIterator.COMPLETED_SEMAPHORE);
                    return INDEX_WORKER_ID;
                }
                LOGGER.trace(indexUrl.toString());
                shouldContinue = processFile(indexUrl, recordProcessor);
            }
            return INDEX_WORKER_ID;
        }

        private boolean processFile(
                FetchEmitTuple fetchEmitTuple, AbstractRecordProcessor recordProcessor)
                throws InterruptedException {
            long start = System.currentTimeMillis();
            LOGGER.info(
                    "starting to fetch index gz: {}", fetchEmitTuple.getFetchKey().getFetchKey());
            try (TikaInputStream tis =
                    (TikaInputStream)
                            indexFetcher.fetch(
                                    fetchEmitTuple.getFetchKey().getFetchKey(),
                                    new Metadata(),
                                    new ParseContext())) {
                try (InputStream is = new BufferedInputStream(new GZIPInputStream(tis))) {
                    try (BufferedReader reader =
                            new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                        String line = reader.readLine();
                        int lines = 0;
                        long elapsed = System.currentTimeMillis() - start;
                        LOGGER.info(
                                "Finished fetching index {} bytes in {} ms for index gz: {}",
                                String.format(Locale.US, "%,d", tis.getLength()),
                                String.format(Locale.US, "%,d", elapsed),
                                fetchEmitTuple.getFetchKey().getFetchKey());
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
            } catch (TikaException | IOException | RuntimeException e) {
                // RuntimeException covers TikaTimeoutException (extends RuntimeException, not
                // TikaException) -- an occasional slow fetch must not kill the whole run.
                LOGGER.error(
                        "failed while processing " + fetchEmitTuple.getFetchKey().getFetchKey(), e);
            }
            long elapsed = System.currentTimeMillis() - start;
            counter.getIndexFilesCompleted().incrementAndGet();
            LOGGER.info(
                    "finished processing index gz in ({}) ms: {} -- {}",
                    String.format(Locale.US, "%,d", elapsed),
                    fetchEmitTuple.getFetchKey().getFetchKey(),
                    counter.progressSummary());
            return true;
        }
    }
}
