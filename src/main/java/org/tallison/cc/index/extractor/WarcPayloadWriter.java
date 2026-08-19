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

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.io.TargetPathRewriter;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.emitter.StreamEmitter;

/**
 * Extracts a payload from a WARC record, verifies its digest against the CC index, computes a
 * target digest (SHA-256), and writes the file via a {@link StreamEmitter}.
 */
class WarcPayloadWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarcPayloadWriter.class);
    private static final Logger EXTRACTED_LOGGER = LoggerFactory.getLogger("extracted-urls");
    private static final Logger EXTRACTED_ALL_LOGGER =
            LoggerFactory.getLogger("extracted-urls-all");

    private final StreamEmitter emitter;
    private final TargetPathRewriter targetPathRewriter;
    private final boolean extractTruncated;
    private final CCIndexReaderCounter counter;
    private final Base32 base32 = new Base32();

    WarcPayloadWriter(
            StreamEmitter emitter,
            TargetPathRewriter targetPathRewriter,
            boolean extractTruncated,
            CCIndexReaderCounter counter) {
        this.emitter = emitter;
        this.targetPathRewriter = targetPathRewriter;
        this.extractTruncated = extractTruncated;
        this.counter = counter;
    }

    /**
     * Extracts the payload from a WARC response record, verifies its SHA-1 digest, computes a
     * SHA-256 target digest, and writes the file.
     */
    void writePayload(String id, CCIndexRecord ccIndexRecord, WarcRecord record)
            throws IOException {
        if (!((record instanceof WarcResponse)
                && record.contentType().base().equals(MediaType.HTTP))) {
            return;
        }

        Optional<WarcPayload> payload = ((WarcResponse) record).payload();
        if (!payload.isPresent()) {
            LOGGER.warn("payload not present {}", id);
            counter.getEmptyPayload().incrementAndGet();
            return;
        }
        if (payload.get().body().size() == 0) {
            LOGGER.warn("payload body size==0 id={}", id);
            counter.getEmptyPayload().incrementAndGet();
            return;
        }

        Path tmp = Files.createTempFile("ccfile-fetcher-", "");
        try {
            Files.copy(payload.get().body().stream(), tmp, StandardCopyOption.REPLACE_EXISTING);
            String base32Sha1 = computeSha1Base32(tmp);
            if (base32Sha1 == null) {
                return;
            }
            if (!base32Sha1.equals(ccIndexRecord.getDigest())) {
                LOGGER.warn(
                        "Bad digest for url={} ccindex={} sha1={}",
                        id,
                        ccIndexRecord.getDigest(),
                        base32Sha1);
            }
            String targetDigest = computeSha256Hex(tmp);
            if (targetDigest == null) {
                return;
            }
            long length = Files.size(tmp);
            String targetPath = targetPathRewriter.rewrite(targetDigest);
            try (InputStream is = TikaInputStream.get(tmp, new Metadata())) {
                emitter.emit(targetPath, is, new Metadata(), new ParseContext());
                logSuccess(ccIndexRecord, targetDigest, length, targetPath);
            } catch (IOException | TikaException e) {
                LOGGER.warn("problem writing id={}", id, e);
            }
        } finally {
            try {
                Files.delete(tmp);
            } catch (IOException e) {
                LOGGER.warn("can't delete {}", tmp.toAbsolutePath(), e);
            }
        }
    }

    private String computeSha1Base32(Path file) {
        try (InputStream is = Files.newInputStream(file)) {
            return base32.encodeAsString(DigestUtils.sha1(is));
        } catch (IOException e) {
            LOGGER.warn("IOException during SHA-1 digesting: {}", file.toAbsolutePath());
            return null;
        }
    }

    private String computeSha256Hex(Path file) {
        try (InputStream is = Files.newInputStream(file)) {
            return DigestUtils.sha256Hex(is);
        } catch (IOException e) {
            LOGGER.warn("IOException during SHA-256 digesting: {}", file.toAbsolutePath());
            return null;
        }
    }

    private void logSuccess(
            CCIndexRecord ccIndexRecord, String targetDigest, long length, String targetPath) {
        if (extractTruncated) {
            EXTRACTED_ALL_LOGGER.info(
                    "",
                    ccIndexRecord.getUrl(),
                    ccIndexRecord.getNormalizedMime(),
                    ccIndexRecord.getNormalizedMimeDetected(),
                    ccIndexRecord.getFilename(),
                    ccIndexRecord.getOffset(),
                    ccIndexRecord.getLength(),
                    ccIndexRecord.getTruncated(),
                    targetDigest,
                    length,
                    targetPath);
        } else {
            EXTRACTED_LOGGER.info(
                    "",
                    ccIndexRecord.getUrl(),
                    ccIndexRecord.getNormalizedMime(),
                    ccIndexRecord.getNormalizedMimeDetected(),
                    ccIndexRecord.getFilename(),
                    ccIndexRecord.getOffset(),
                    ccIndexRecord.getLength(),
                    targetDigest,
                    length,
                    targetPath);
        }
    }
}
