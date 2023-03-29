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

import java.nio.file.Path;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.tallison.cc.index.IndexIterator;
import org.tallison.cc.index.io.BackoffHttpFetcher;
import org.tallison.cc.index.io.TargetPathRewriter;
import org.tallison.cc.index.selector.RecordSelector;

import org.apache.tika.config.Initializable;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.emitter.fs.FileSystemEmitter;
import org.apache.tika.pipes.emitter.s3.S3Emitter;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.fs.FileSystemFetcher;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;
import org.apache.tika.utils.StringUtils;

public class ExtractorConfig {

    public static String CC_HTTPS_BASE = "https://data.commoncrawl.org";

    public static String CC_S3_BUCKET = "commoncrawl";

    public static String CC_REGION = "us-east-1";

    public static String DEFAULT_FS_DOCS_PATH = "docs";

    public static long[] DEFAULT_THROTTLE_SECONDS = new long[]{30, 120, 600, 1800};
    private int numThreads = 2;
    //maximum records to read
    private long maxRecords = -1;

    //maximum files extracted from cc
    private long maxFilesExtracted = -1;
    //maximum files written to 'truncated' logger
    private long maxFilesTruncated = -1;

    private Path indexPathsFile;
    private String targetPathPattern = "";

    private boolean dryRun = false;

    private boolean extractTruncated = false;

    private RecordSelector recordSelector = RecordSelector.ACCEPT_ALL_RECORDS;

    @JsonProperty("indices")
    private IndexIterator indexIterator;

    @JsonProperty("fetcher")
    private FetchConfig fetchConfig;

    @JsonProperty("docs")
    private EmitConfig emitConfig;

    public static String getCcHttpsBase() {
        return CC_HTTPS_BASE;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public long getMaxRecords() {
        return maxRecords;
    }

    public void setMaxRecords(long maxRecords) {
        this.maxRecords = maxRecords;
    }

    public long getMaxFilesExtracted() {
        return maxFilesExtracted;
    }

    public void setMaxFilesExtracted(long maxFilesExtracted) {
        this.maxFilesExtracted = maxFilesExtracted;
    }

    public long getMaxFilesTruncated() {
        return maxFilesTruncated;
    }

    public void setMaxFilesTruncated(long maxFilesTruncated) {
        this.maxFilesTruncated = maxFilesTruncated;
    }

    public Path getIndexPathsFile() {
        return indexPathsFile;
    }

    public void setIndexPathsFile(Path indexPathsFile) {
        this.indexPathsFile = indexPathsFile;
    }


    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public String getTargetPathPattern() {
        return targetPathPattern;
    }

    public void setTargetPathPattern(String pathPattern) {
        this.targetPathPattern = pathPattern;
    }

    public TargetPathRewriter getTargetPathRewriter() {
        return new TargetPathRewriter(targetPathPattern);
    }

    public RecordSelector getRecordSelector() {
        return recordSelector;
    }

    public void setRecordSelector(RecordSelector recordSelector) {
        this.recordSelector = recordSelector;
    }

    public IndexIterator getIndexIterator() {
        return indexIterator;
    }

    public Fetcher newFetcher() throws TikaConfigException {
        return fetchConfig.newFetcher();
    }

    public StreamEmitter newEmitter() throws TikaConfigException {
        if (emitConfig == null) {
            emitConfig = new EmitConfig(DEFAULT_FS_DOCS_PATH);
        }
        return emitConfig.newEmitter();
    }

    public void setExtractTruncated(boolean extractTruncated) {
        this.extractTruncated = extractTruncated;
    }
    public boolean isExtractTruncated() {
        return extractTruncated;
    }

    private static class FetchConfig {
        private final String profile;
        private final long[] throttleSeconds;
        private final String basePath;

        @JsonCreator
        public FetchConfig(@JsonProperty("profile") String profile,
                           @JsonProperty("throttleSeconds") long[] throttleSeconds,
                           @JsonProperty("basePath") String basePath) {
            this.profile = profile;
            this.throttleSeconds =
                    (throttleSeconds == null) ? DEFAULT_THROTTLE_SECONDS : throttleSeconds;
            this.basePath = basePath;
        }

        Fetcher newFetcher() throws TikaConfigException {
            Fetcher fetcher;
            if (profile != null) {
                fetcher = new S3Fetcher();
                ((S3Fetcher) fetcher).setProfile(profile);
                ((S3Fetcher) fetcher).setCredentialsProvider("profile");
                ((S3Fetcher) fetcher).setBucket(ExtractorConfig.CC_S3_BUCKET);
                ((S3Fetcher) fetcher).setRegion(ExtractorConfig.CC_REGION);
                //Update and make configurable once TIKA-3993 is fixed
                ((S3Fetcher) fetcher).setRetries(3);
                ((S3Fetcher) fetcher).setSleepBeforeRetryMillis(30000);
            } else if (basePath != null) {
                fetcher = new FileSystemFetcher();
                ((FileSystemFetcher) fetcher).setBasePath(basePath);
            } else {
                fetcher = new BackoffHttpFetcher(throttleSeconds);
            }
            if (fetcher instanceof Initializable) {
                ((Initializable) fetcher).initialize(Collections.EMPTY_MAP);
            }
            return fetcher;
        }
    }

    private static class EmitConfig {
        private String profile;
        private String region;
        private String bucket;
        private String prefix;
        private String path;

        private EmitConfig(String path) {
            this.path = path;
        }

        //TODO -- clean this up with different classes
        //for the different fetchers and use jackson's inference
        @JsonCreator
        public EmitConfig(@JsonProperty("profile") String profile,
                          @JsonProperty("region") String region,
                          @JsonProperty("bucket") String bucket,
                          @JsonProperty("prefix") String prefix,
                          @JsonProperty("path") String path) {
            this.profile = profile;
            this.region = region;
            this.bucket = bucket;
            this.prefix = prefix;
            this.path = path;
        }

        public StreamEmitter newEmitter() throws TikaConfigException {
            if (!StringUtils.isBlank(profile)) {
                S3Emitter emitter = new S3Emitter();
                emitter.setCredentialsProvider("profile");
                emitter.setProfile(profile);

                if (StringUtils.isBlank(bucket)) {
                    throw new TikaConfigException("Must specify a bucket for docs");
                }
                emitter.setBucket(bucket);
                if (region != null) {
                    emitter.setRegion(region);
                } else {
                    emitter.setRegion(ExtractorConfig.CC_REGION);
                }
                if (!StringUtils.isBlank(prefix)) {
                    emitter.setPrefix(prefix);
                }
                emitter.setFileExtension("");
                emitter.initialize(Collections.EMPTY_MAP);
                return emitter;
            }
            if (StringUtils.isBlank(path)) {
                path = DEFAULT_FS_DOCS_PATH;
            }
            FileSystemEmitter emitter = new FileSystemEmitter();
            emitter.setBasePath(path);
            emitter.setOnExists("skip");
            emitter.setFileExtension("");
            return emitter;
        }
    }
}
