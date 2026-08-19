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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import org.tallison.cc.index.AbstractRecordProcessor;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.pipesiterator.PipesIterator;
import org.apache.tika.utils.StringUtils;

class IndexWorker implements Callable<Long> {

    /**
     * Sentinel value returned by workers so the orchestrator can distinguish worker completion from
     * the index-reader completion (which returns 1L).
     */
    static final Long INDEX_WORKER_ID = 42L;

    /**
     * Maximum time a worker will wait for the next index file path before assuming the pipeline is
     * stalled and throwing a TimeoutException.
     */
    private static final long POLL_TIMEOUT_MINUTES = 120;

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexWorker.class);

    private final ArrayBlockingQueue<FetchEmitTuple> indexFileQueue;
    private final AbstractRecordProcessor recordProcessor;
    private final Fetcher indexFileFetcher;

    IndexWorker(
            ExtractorConfig fetcherConfig,
            ArrayBlockingQueue<FetchEmitTuple> indexFileQueue,
            AbstractRecordProcessor recordProcessor)
            throws TikaException {
        this.indexFileQueue = indexFileQueue;
        this.recordProcessor = recordProcessor;
        this.indexFileFetcher = fetcherConfig.newIndexFileFetcher();
    }

    @Override
    public Long call() throws Exception {
        boolean shouldContinue = true;
        while (shouldContinue) {

            FetchEmitTuple indexUrl = indexFileQueue.poll(POLL_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (indexUrl == null) {
                throw new TimeoutException(
                        "waited " + POLL_TIMEOUT_MINUTES + " minutes for a new record");
            }

            if (indexUrl == PipesIterator.COMPLETED_SEMAPHORE) {
                recordProcessor.close();
                // can hang forever
                indexFileQueue.put(PipesIterator.COMPLETED_SEMAPHORE);
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
        LOGGER.info("starting to fetch index gz: {}", fetchEmitTuple.getFetchKey().getFetchKey());
        try (TikaInputStream tis =
                (TikaInputStream)
                        indexFileFetcher.fetch(
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
        } catch (TikaException | IOException e) {
            LOGGER.error(
                    "failed while processing " + fetchEmitTuple.getFetchKey().getFetchKey(), e);
        }
        long elapsed = System.currentTimeMillis() - start;
        LOGGER.info(
                "finished processing index gz in ({}) ms: {}",
                String.format(Locale.US, "%,d", elapsed),
                fetchEmitTuple.getFetchKey().getFetchKey());
        return true;
    }
}
