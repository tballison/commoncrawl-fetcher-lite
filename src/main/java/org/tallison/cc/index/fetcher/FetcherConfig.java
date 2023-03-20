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

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.emitter.fs.FileSystemEmitter;
import org.tallison.cc.index.io.HTTPFetchWrapper;
import org.tallison.cc.index.io.TargetPathRewriter;
import org.tallison.cc.index.selector.RecordSelector;

public class FetcherConfig {

    public static String CC_HTTPS_BASE = "https://data.commoncrawl.org/";
    private int numThreads = 2;
    //maximum records to read
    private long maxRecords = -1;

    //maximum files extracted from cc
    private long maxFilesExtracted = -1;
    //maximum files written to 'truncated' list.
    private long maxFilesTruncated = -1;

    private Path indexPathsFile;

    private Path filesDirectory = Paths.get("docs");

    private Path truncatedUrlsFile = Paths.get("urls-for-truncated-files.txt");

    private String targetPathPattern = "";

    private boolean dryRun = false;

    private long[] throttleSeconds = HTTPFetchWrapper.DEFAULT_THROTTLE_SECONDS;

    private RecordSelector recordSelector = RecordSelector.ACCEPT_ALL_RECORDS;

    public static String getCcHttpsBase() {
        return CC_HTTPS_BASE;
    }

    public static void setCcHttpsBase(String ccHttpsBase) {
        CC_HTTPS_BASE = ccHttpsBase;
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

    public void setFilesDirectory(Path filesDirectory) {
        this.filesDirectory = filesDirectory;
    }

    public StreamEmitter getEmitter() {
        if (filesDirectory != null) {
            FileSystemEmitter emitter = new FileSystemEmitter();
            emitter.setBasePath(filesDirectory.toAbsolutePath().toString());
            emitter.setOnExists("replace");
            return emitter;
        } else {
            throw new IllegalStateException("must set 'filesDirectory'");
        }

    }
    public Path getTruncatedUrlsFile() {
        return truncatedUrlsFile;
    }

    public void setTruncatedUrlsFile(Path truncatedUrlsFile) {
        this.truncatedUrlsFile = truncatedUrlsFile;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setRecordSelector(RecordSelector recordSelector) {
        this.recordSelector = recordSelector;
    }

    public void setTargetPathPattern(String pathPattern) {
        this.targetPathPattern = pathPattern;
    }

    public String getTargetPathPattern() {
        return targetPathPattern;
    }

    public TargetPathRewriter getTargetPathRewriter() {
        return new TargetPathRewriter(targetPathPattern);
    }

    public RecordSelector getRecordSelector() {
        return recordSelector;
    }

    public long[] getThrottleSeconds() {
        return throttleSeconds;
    }
}
