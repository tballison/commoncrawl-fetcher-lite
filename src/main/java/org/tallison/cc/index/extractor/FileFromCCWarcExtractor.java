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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.io.TargetPathRewriter;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.RangeFetcher;

public class FileFromCCWarcExtractor {
    private static Logger LOGGER =
            LoggerFactory.getLogger(FileFromCCWarcExtractor.class);
    private static Logger EXTRACTED_LOGGER = LoggerFactory.getLogger("extracted-urls");
    private static Logger EXTRACTED_ALL_LOGGER = LoggerFactory.getLogger("extracted-urls-all");
    private final StreamEmitter emitter;
    private final TargetPathRewriter targetPathRewriter;

    private RangeFetcher fetcher;
    private final boolean extractTruncated;
    private Base32 base32 = new Base32();

    private final CCIndexReaderCounter ccIndexReaderCounter;
    public FileFromCCWarcExtractor(ExtractorConfig fetcherConfig,
                                   CCIndexReaderCounter ccIndexReaderCounter) throws TikaConfigException {
        this.emitter = fetcherConfig.newEmitter();
        this.fetcher = (RangeFetcher) fetcherConfig.newFetcher();
        this.targetPathRewriter = fetcherConfig.getTargetPathRewriter();
        this.extractTruncated = fetcherConfig.isExtractTruncated();
        this.ccIndexReaderCounter = ccIndexReaderCounter;
    }

    public void fetchToPath(CCIndexRecord record) throws InterruptedException {

        LOGGER.debug("going to fetch {} {}->{}", record.getFilename(), record.getOffset(),
                record.getLength());
        FetchEmitTuple t = new FetchEmitTuple(record.getFilename(),
                new FetchKey("", record.getFilename(), record.getOffset(),
                        record.getOffset() + record.getLength() - 1), new EmitKey());
        byte[] warcRecordGZBytes;
        try {
            warcRecordGZBytes = fetchWarcBytes(t);
        } catch (TikaException | IOException e) {
            LOGGER.warn("couldn't get bytes from cc's warc " + t, e);
            return;
        }
        String id = record.getUrl();
        try {
            parseWarc(id, record, warcRecordGZBytes);
        } catch (IOException e) {
            LOGGER.warn("problem parsing warc file", e);
        }
    }


    private void fetchPayload(String id, CCIndexRecord ccIndexRecord, WarcRecord record)
            throws IOException {
        if (!((record instanceof WarcResponse) &&
                record.contentType().base().equals(MediaType.HTTP))) {
            return;
        }

        Optional<WarcPayload> payload = ((WarcResponse) record).payload();
        if (!payload.isPresent()) {
            LOGGER.warn("payload not present {}", id);
            ccIndexReaderCounter.getEmptyPayload().incrementAndGet();
            return;
        }
        if (payload.get().body().size() == 0) {
            LOGGER.warn("payload body size==0 id={}", id);
            ccIndexReaderCounter.getEmptyPayload().incrementAndGet();
            return;
        }

        Path tmp = Files.createTempFile("ccfile-fetcher-", "");

        try {
            Files.copy(payload.get().body().stream(), tmp, StandardCopyOption.REPLACE_EXISTING);
            String targetDigest = null;
            String base32Sha1 = "";
            try (InputStream is = Files.newInputStream(tmp)) {
                base32Sha1 = base32.encodeAsString(DigestUtils.sha1(is));
            } catch (IOException e) {
                LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
                return;
            }
            if (!base32Sha1.equals(ccIndexRecord.getDigest())) {
                LOGGER.warn("Bad digest for url={} ccindex={} sha1={}", id,
                        ccIndexRecord.getDigest(), base32Sha1);
            }
            //TODO: make digest and encoding configurable
            try (InputStream is = Files.newInputStream(tmp)) {
                targetDigest = DigestUtils.sha256Hex(is);
            } catch (IOException e) {
                LOGGER.warn("IOException during digesting: " + tmp.toAbsolutePath());
                return;
            }
            long length = -1;
            try {
                length = Files.size(tmp);
            } catch (IOException e) {
                LOGGER.warn("IOException getting temp file length: " + tmp.toAbsolutePath());
                return;
            }
            String targetPath = targetPathRewriter.rewrite(targetDigest);
            Metadata metadata = new Metadata();
            try (InputStream is = TikaInputStream.get(tmp, metadata)) {
                emitter.emit(targetPath, is, new Metadata(), new ParseContext());
                logSuccess(ccIndexRecord, targetDigest, length, targetPath);
            } catch (IOException | TikaException e) {
                LOGGER.warn("problem writing id=" + id, e);
            }
        } finally {
            try {
                Files.delete(tmp);
            } catch (IOException e) {
                LOGGER.warn("can't delete " + tmp.toAbsolutePath(), e);
            }
        }
    }

    private void logSuccess(CCIndexRecord ccIndexRecord, String targetDigest, long length,
                            String targetPath) {
        if (extractTruncated) {
            EXTRACTED_ALL_LOGGER.info("", ccIndexRecord.getUrl(),
                    ccIndexRecord.getNormalizedMime(),
                    ccIndexRecord.getNormalizedMimeDetected(),
                    ccIndexRecord.getFilename(),
                    ccIndexRecord.getOffset(), ccIndexRecord.getLength(),
                    ccIndexRecord.getTruncated(), targetDigest, length,
                    targetPath);
        } else {
            //new ObjectArray ?
            //url,mime_detected,warc_file,warc_offset,warc_length,sha256,length,path
            EXTRACTED_LOGGER.info("", ccIndexRecord.getUrl(),
                    ccIndexRecord.getNormalizedMime(),
                    ccIndexRecord.getNormalizedMimeDetected(),
                    ccIndexRecord.getFilename(),
                    ccIndexRecord.getOffset(), ccIndexRecord.getLength(),
                    targetDigest, length,
                    targetPath);
        }


    }

    private void parseWarc(String id, CCIndexRecord ccIndexRecord, byte[] warcRecordGZBytes)
            throws IOException {
        //need to leave initial inputstream open while parsing warcrecord
        //can't just parse record and return
        try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(warcRecordGZBytes))) {
            try (WarcReader warcreader = new WarcReader(is)) {

                //should be a single warc per file
                //return the first
                for (WarcRecord warcRecord : warcreader) {
                    fetchPayload(id, ccIndexRecord, warcRecord);
                    return;
                }
            }
        }
    }

    private byte[] fetchWarcBytes(FetchEmitTuple t)
            throws TikaException, InterruptedException, IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        FetchKey k = t.getFetchKey();
        try (InputStream is = fetcher.fetch(k.getFetchKey(), k.getRangeStart(), k.getRangeEnd(),
                new Metadata())) {
            IOUtils.copy(is, bos);
        }
        return bos.toByteArray();
    }

}
