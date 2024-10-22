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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.AbstractRecordProcessor;
import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.CCIndexRecord;
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
 * This counts mime_detected.  Use a regular file selector to include
 * only urls that had a 200, e.g.
 */
public class CCMimeCounter {

    private static final Long INDEX_WORKER_ID = 42L;
    private static final Long INDEX_READER_ID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CCMimeCounter.class);
    private static final int BATCH_SIZE = 50000;

    public static void main(String[] args) throws Exception {
        ExtractorConfig fetcherConfig = new ObjectMapper().readValue(new File(args[0]), ExtractorConfig.class);
        execute(fetcherConfig);
    }

    private static void execute(ExtractorConfig fetcherConfig) throws IOException, TikaException {
        ArrayBlockingQueue<FetchEmitTuple> indexPathsList = new ArrayBlockingQueue<>(1000);
        //IndexPathsReader reads a file containing a list of cc-index.paths files
        //and writes the literal gz files (cc-index/collections/CC-MAIN-2023-06/indexes/cdx-00000.gz)
        //to indexPathsList


        //IndexWorker reads a single index.gz file at a time and processes each record
        //It fetches non truncated files and logs truncated files
        int totalThreads = fetcherConfig.getNumThreads() + 1;

        ExecutorService executorService = Executors.newFixedThreadPool(totalThreads);
        ExecutorCompletionService<Long> executorCompletionService = new ExecutorCompletionService<>(executorService);

        IndexIterator indexIterator = fetcherConfig.getIndexIterator();
        indexIterator.initialize(Collections.EMPTY_MAP);
        executorCompletionService.submit(new CallablePipesIterator(indexIterator, indexPathsList));
        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        int finishedWorkers = 0;
        List<DetectedMimeCounter> detectedMimeCounters = new ArrayList<>();
        try {
            for (int i = 0; i < fetcherConfig.getNumThreads(); i++) {
                DetectedMimeCounter processor = new DetectedMimeCounter(fetcherConfig, counter);
                detectedMimeCounters.add(processor);
                executorCompletionService.submit(new IndexWorker(fetcherConfig, indexPathsList, processor));
            }


            while (finishedWorkers < fetcherConfig.getNumThreads()) {
                //blocking
                Future<Long> future = executorCompletionService.take();
                if (future != null) {
                    Long f = future.get();
                    LOGGER.debug("completed worker or reader value={}", f);
                    if (f.equals(INDEX_WORKER_ID)) {
                        finishedWorkers++;
                    } else if (f.equals(INDEX_READER_ID)) {
                        LOGGER.info("Index paths reader successfully completed");
                    }
                }
            }
        } catch (TikaConfigException | ExecutionException e) {
            LOGGER.error("main loop exception", e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            LOGGER.warn("main loop interrupted exception", e);
            throw new RuntimeException(e);
        } catch (Throwable t) {
            LOGGER.error("Serious problem", t);
            throw t;
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
        LOGGER.info("finished processing; now off to writing reports");
        summarize(detectedMimeCounters);
    }

    private static void summarize(List<DetectedMimeCounter> detectedMimeCounters) throws IOException {
        Map<String, Long> total = new HashMap<>();
        Map<String, Long> truncated = new HashMap<>();
        Map<String, Long> nonTruncated = new HashMap<>();
        for (DetectedMimeCounter c : detectedMimeCounters) {
            update(c.totalCounts, total);
            update(c.truncatedCounts, truncated);
        }
        calcNonTruncated(truncated, total, nonTruncated);
        report("total", total);
        report("truncated", truncated);
        report("non-truncated", nonTruncated);
    }

    private static void calcNonTruncated(Map<String, Long> truncated,
                                         Map<String, Long> total, Map<String, Long> nonTruncated) {
        for (Map.Entry<String, Long> e : total.entrySet()) {
            Long val = e.getValue();
            Long t = truncated.getOrDefault(e.getKey(), 0l);
            val -= t;
            nonTruncated.put(e.getKey(), val);
        }
    }

    private static void report(String name, Map<String, Long> m) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                Paths.get(name + ".csv"), StandardCharsets.UTF_8)) {
            try (CSVPrinter printer = new CSVPrinter(writer, CSVFormat.EXCEL)) {
                printer.printRecord("mime", "count");
                m
                        .entrySet()
                        .stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .forEach(e -> {
                            try {
                                printer.printRecord(e.getKey(), e.getValue());
                            } catch (IOException ex) {
                                throw new RuntimeException(ex);
                            }
                        });
            }
        }
    }

    private static void update(Map<String, MutableLong> from, Map<String, Long> to) {
        for (Map.Entry<String, MutableLong> e : from.entrySet()) {
            Long cnt = to.get(e.getKey());
            if (cnt == null) {
                cnt = 0l;
            }
            cnt += e
                    .getValue()
                    .getValue();
            to.put(e.getKey(), cnt);
        }
    }

    private static class IndexWorker implements Callable<Long> {

        private final ArrayBlockingQueue<FetchEmitTuple> indexUrls;
        private final AbstractRecordProcessor recordProcessor;

        private final Fetcher fetcher;

        IndexWorker(ExtractorConfig fetcherConfig, ArrayBlockingQueue<FetchEmitTuple> indexUrls,
                    AbstractRecordProcessor recordProcessor) throws TikaException {
            this.indexUrls = indexUrls;
            this.recordProcessor = recordProcessor;
            this.fetcher = fetcherConfig.newIndexFetcher();
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
                    //can hang forever
                    indexUrls.put(PipesIterator.COMPLETED_SEMAPHORE);
                    return INDEX_WORKER_ID;
                }
                shouldContinue = processFile(indexUrl, recordProcessor);
            }
            return INDEX_WORKER_ID;
        }

        private boolean processFile(FetchEmitTuple fetchEmitTuple,
                                    AbstractRecordProcessor recordProcessor) throws InterruptedException {
            long start = System.currentTimeMillis();
            LOGGER.info("starting to fetch index gz path={} with fetcher class={}", fetchEmitTuple
                    .getFetchKey()
                    .getFetchKey(), fetcher.getClass());
            try (TikaInputStream tis = (TikaInputStream) fetcher.fetch(fetchEmitTuple
                    .getFetchKey()
                    .getFetchKey(), new Metadata(), new ParseContext())) {
                try (InputStream is = new BufferedInputStream(new GZIPInputStream(tis))) {
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(is, StandardCharsets.UTF_8))) {
                        int lineCount = 0;
                        long elapsed = System.currentTimeMillis() - start;
                        LOGGER.info("Finished fetching {} bytes in {} ms for index gz: {}",
                                String.format(Locale.US, "%,d", tis.getLength()),
                                String.format(Locale.US, "%,d", elapsed), fetchEmitTuple
                                        .getFetchKey()
                                        .getFetchKey());
                        List<String> lines = new ArrayList<>();
                        String line = reader.readLine();
                        while (line != null) {
                            if (StringUtils.isBlank(line)) {
                                line = reader.readLine();
                                continue;
                            }
                            lines.add(line);
                            if (lines.size() >= BATCH_SIZE) {
                                boolean shouldContinue = processLines(lines, recordProcessor);
                                if (!shouldContinue) {
                                    return shouldContinue;
                                }
                                lines.clear();
                            }
                            line = reader.readLine();
                        }
                        boolean shouldContinue = processLines(lines, recordProcessor);
                        if (!shouldContinue) {
                            return shouldContinue;
                        }
                    }
                }
            } catch (TikaException | IOException e) {
                LOGGER.error("failed while processing " + fetchEmitTuple
                        .getFetchKey()
                        .getFetchKey(), e);
            }
            long elapsed = System.currentTimeMillis() - start;
            LOGGER.info("finished processing index gz in ({}) ms: {}",
                    String.format(Locale.US, "%,d", elapsed), fetchEmitTuple
                    .getFetchKey()
                    .getFetchKey());
            return true;
        }

        private boolean processLines(List<String> lines,
                                     AbstractRecordProcessor recordProcessor) throws InterruptedException {
            for (String line : lines) {
                try {
                    boolean shouldContinue = recordProcessor.process(line);
                    if (!shouldContinue) {
                        return shouldContinue;
                    }
                } catch (IOException e) {
                    LOGGER.warn("bad json: " + line);
                }
            }
            return true;
        }
    }

    private static class DetectedMimeCounter extends AbstractRecordProcessor {
        private final ExtractorConfig fetcherConfig;
        private final CCIndexReaderCounter counter;
        private final Map<String, MutableLong> totalCounts = new HashMap<>();
        private final Map<String, MutableLong> truncatedCounts = new HashMap<>();

        public DetectedMimeCounter(ExtractorConfig fetcherConfig, CCIndexReaderCounter counter) {
            this.fetcherConfig = fetcherConfig;
            this.counter = counter;
        }

        @Override
        public boolean process(String json) throws IOException, InterruptedException {
            long totalRead = counter
                    .getRecordsRead()
                    .incrementAndGet();
            if (totalRead % 1000000 == 0) {
                LOGGER.info("processed: {}", String.format(Locale.US,"%,d", totalRead));
            }
            if (fetcherConfig.getMaxRecords() > -1 && totalRead >= fetcherConfig.getMaxRecords()) {
                LOGGER.info("hit max read");
                return false;
            }
            //check for hit max
            //return false;

            Optional<CCIndexRecord> record = CCIndexRecord.parseRecord(json);
            if (record.isEmpty()) {
                //problem already logged
                return true;
            }
            CCIndexRecord r = record.get();
            if (!fetcherConfig
                    .getRecordSelector()
                    .select(r)) {
                return true;
            }
            increment(totalCounts, r.getNormalizedMimeDetected());
            if (!StringUtils.isBlank(r.getTruncated())) {
                long truncated = counter
                        .getTruncated()
                        .incrementAndGet();
                if (fetcherConfig.getMaxFilesTruncated() > -1 &&
                        truncated >= fetcherConfig.getMaxFilesTruncated()) {
                    LOGGER.info("hit max truncated files");
                    return false;
                }
                increment(truncatedCounts, r.getNormalizedMimeDetected());
                return true;
            }
            return true;
        }

        private void increment(Map<String, MutableLong> m, String k) {
            MutableLong cnt = m.get(k);
            if (cnt == null) {
                cnt = new MutableLong(1);
                m.put(k, cnt);
                return;
            } else {
                cnt.increment();
            }
        }

        @Override
        public void close() throws IOException {

        }
    }
}
