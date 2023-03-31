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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.AbstractRecordProcessor;
import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.CCIndexRecord;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.utils.StringUtils;

public class CCFileExtractorRecordProcessor extends AbstractRecordProcessor {

    private static Logger LOGGER = LoggerFactory.getLogger(CCFileExtractorRecordProcessor.class);
    private static Logger TRUNCATED_URLS_LOGGER = LoggerFactory.getLogger("truncated-urls");
    private static Logger TRUNCATED_URLS_FULL_LOGGER =
            LoggerFactory.getLogger("truncated-urls-full");

    private final ExtractorConfig fetcherConfig;
    private final CCIndexReaderCounter counter;

    private final FileFromCCWarcExtractor fileFromCCWarcFetcher;

    private long reportEvery = 100000;

    public CCFileExtractorRecordProcessor(ExtractorConfig fetcherConfig, CCIndexReaderCounter counter)
            throws TikaConfigException, IOException {
        this.fetcherConfig = fetcherConfig;
        this.counter = counter;
        this.fileFromCCWarcFetcher = new FileFromCCWarcExtractor(fetcherConfig, counter);
        //completely arbitrary
        if (fetcherConfig.getNumThreads() > 10) {
            reportEvery = 1000000;
        }
    }

    @Override
    public boolean process(String json) throws IOException, InterruptedException {
        long totalRead = counter.getRecordsRead().incrementAndGet();
        if (totalRead % reportEvery == 0) {
            LOGGER.info("processed: {}", counter);
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
        if (!fetcherConfig.getRecordSelector().select(r)) {
            return true;
        }
        //if truncated, count appropriately and test for limits
        if (!StringUtils.isBlank(r.getTruncated())) {
            long truncated = counter.getTruncated().incrementAndGet();
            if (fetcherConfig.getMaxFilesTruncated() > -1 &&
                    truncated >= fetcherConfig.getMaxFilesTruncated()) {
                LOGGER.info("hit max truncated files");
                return false;
            }
        }

        if (fetcherConfig.isExtractTruncated() || StringUtils.isBlank(r.getTruncated())) {
            long extracted = counter.getFilesExtracted().incrementAndGet();
            if (fetcherConfig.getMaxFilesExtracted() > -1 &&
                    extracted >= fetcherConfig.getMaxFilesExtracted()) {
                LOGGER.info("hit max extracted files");
                return false;
            }
            if (fetcherConfig.isDryRun()) {
                LOGGER.info("dry run, but would have extracted {}", r);
                return true;
            }
            fetchBytes(r);
            return true;
        } else {
            String url = r.getUrl();
            TRUNCATED_URLS_LOGGER.info("", url);
            //url,mime,mime_detected,warc_file,warc_offset,warc_length,truncated
            TRUNCATED_URLS_FULL_LOGGER.info("", url,
                    r.getNormalizedMime(), r.getNormalizedMimeDetected(), r.getFilename(),
                    r.getOffset(), r.getLength(), r.getTruncated());
            return true;
        }
    }

    private void fetchBytes(CCIndexRecord r) throws InterruptedException {
        fileFromCCWarcFetcher.fetchToPath(r);
    }

    @Override
    public void close() throws IOException {

    }
}
