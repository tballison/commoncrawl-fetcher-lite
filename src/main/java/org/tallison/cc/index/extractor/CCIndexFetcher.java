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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.IndexIterator;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.pipesiterator.CallablePipesIterator;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

/**
 * This class fetches index files from aws to a local file share.
 * <p>
 * This pulls the index files either via https or s3
 */
public class CCIndexFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(CCIndexFetcher.class);

    public static void main(String[] args) throws Exception {
        ExtractorConfig fetcherConfig =
                new ObjectMapper().readValue(new File(args[0]), ExtractorConfig.class);
        execute(fetcherConfig);
    }

    private static void execute(ExtractorConfig fetcherConfig) throws Exception {
        ArrayBlockingQueue<FetchEmitTuple> indexPathsList = new ArrayBlockingQueue<>(1000);
        //IndexPathsReader reads a file containing a list of cc-index.paths files
        //and writes the literal gz files (cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00000.gz)
        //to indexPathsList

        int totalThreads = fetcherConfig.getNumThreads() + 1;

        ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
        ExecutorCompletionService<Long> executorCompletionService =
                new ExecutorCompletionService<>(executorService);

        IndexIterator indexIterator = fetcherConfig.getIndexIterator();
        indexIterator.initialize(Collections.EMPTY_MAP);
        executorCompletionService.submit(new CallablePipesIterator(indexIterator, indexPathsList));
        int finishedWorkers = 0;
        try {
            for (int i = 0; i < fetcherConfig.getNumThreads(); i++) {
                executorCompletionService.submit(new IndexFetcher(fetcherConfig, indexPathsList));
            }

            while (finishedWorkers < totalThreads) {
                //blocking
                Future<Long> future = executorCompletionService.take();
                if (future != null) {
                    Long f = future.get();
                    finishedWorkers++;
                    LOGGER.debug("completed {}: {}", f, finishedWorkers);
                }
            }
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
    }

    private static class IndexFetcher implements Callable<Long> {

        private final ExtractorConfig fetcherConfig;
        private final ArrayBlockingQueue<FetchEmitTuple> indexPathsList;

        public IndexFetcher(ExtractorConfig fetcherConfig,
                            ArrayBlockingQueue<FetchEmitTuple> indexPathsList) {
            this.fetcherConfig = fetcherConfig;
            this.indexPathsList = indexPathsList;
        }

        @Override
        public Long call() throws Exception {
            Fetcher fetcher = fetcherConfig.newFetcher();
            StreamEmitter streamEmitter = fetcherConfig.newEmitter();
            while (true) {
                FetchEmitTuple t = indexPathsList.poll(120, TimeUnit.MINUTES);
                if (t == null) {
                    throw new TimeoutException("waited 120 minutes for a new record");
                }

                if (t == PipesIterator.COMPLETED_SEMAPHORE) {
                    indexPathsList.put(PipesIterator.COMPLETED_SEMAPHORE);
                    LOGGER.info("Index fetcher finished");
                    return 1l;
                }
                fetch(t, fetcher, streamEmitter);
            }
        }

        private void fetch(FetchEmitTuple t, Fetcher fetcher, StreamEmitter streamEmitter) {

            LOGGER.info("about to download: " + t.getFetchKey().getFetchKey());
            try (InputStream is = fetcher.fetch(t.getFetchKey().getFetchKey(), new Metadata(), new ParseContext())) {
                streamEmitter.emit(t.getFetchKey().getFetchKey(), is, new Metadata(), new ParseContext());
                LOGGER.info("successfully downloaded: " + t.getFetchKey().getFetchKey());
            } catch (TikaException | IOException e) {
                LOGGER.error("failed to copy " + t.getFetchKey().getFetchKey(), e);
            }
        }
    }
}
