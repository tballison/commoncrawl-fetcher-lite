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
package org.tallison.cc.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.tallison.cc.index.extractor.ExtractorConfig;
import org.tallison.cc.index.io.BackoffHttpFetcher;

import org.apache.tika.config.Initializable;
import org.apache.tika.config.Param;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.fs.FileSystemFetcher;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class IndexIterator extends PipesIterator implements Initializable {

    // temporary storage of the paths this class was constructed with.
    // During initialization, we figure out if these are index list paths
    // or literal index file paths.
    private final List<String> configuredPaths = new ArrayList<>();
    private final List<String> resolvedIndexFiles = new ArrayList<>();

    private Fetcher fetcher = null;

    int maxIndexFiles = -1;
    int skipIndexFiles = 0;

    @JsonCreator
    public IndexIterator(
            @JsonProperty("profile") String profile,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("paths") List<String> indexPaths,
            @JsonProperty("maxIndexFiles") Integer maxIndexFiles,
            @JsonProperty("skipIndexFiles") Integer skipIndexFiles) {
        if (profile != null) {
            fetcher = new S3Fetcher();
            ((S3Fetcher) fetcher).setProfile(profile);
            ((S3Fetcher) fetcher).setCredentialsProvider("profile");
            ((S3Fetcher) fetcher).setBucket(ExtractorConfig.CC_S3_BUCKET);
            ((S3Fetcher) fetcher).setRegion(ExtractorConfig.CC_REGION);
        } else if (basePath != null) {
            fetcher = new FileSystemFetcher();
            ((FileSystemFetcher) fetcher).setBasePath(basePath);
        } else {
            // do nothing
        }
        if (indexPaths != null) {
            configuredPaths.addAll(indexPaths);
        }

        if (maxIndexFiles != null) {
            this.maxIndexFiles = maxIndexFiles;
        }
        if (skipIndexFiles != null) {
            this.skipIndexFiles = skipIndexFiles;
        }
    }

    private static void resolveIndexList(
            Fetcher fetcher, String path, List<String> resolvedIndexFiles)
            throws IOException, TikaException {

        try (InputStream is = fetcher.fetch(path, new Metadata(), new ParseContext())) {
            try (BufferedReader reader = getReader(is, path)) {
                String line = reader.readLine();
                while (line != null) {
                    if (line.startsWith("#") || !line.endsWith(".gz")) {
                        // skip comments and paths that do not end in .gz
                        line = reader.readLine();
                        continue;
                    }
                    resolvedIndexFiles.add(line);
                    line = reader.readLine();
                }
            }
        }
    }

    private static BufferedReader getReader(InputStream is, String path) throws IOException {
        if (path.endsWith(".gz")) {
            return new BufferedReader(
                    new InputStreamReader(new GZIPInputStream(is), StandardCharsets.UTF_8));
        } else {
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        }
    }

    @Override
    protected void enqueue() throws IOException, TimeoutException, InterruptedException {
        int added = 0;
        int skipped = 0;
        for (String p : resolvedIndexFiles) {
            if (skipped < skipIndexFiles) {
                skipped++;
                continue;
            }
            FetchEmitTuple t = new FetchEmitTuple(p, new FetchKey("", p), new EmitKey());
            tryToAdd(t);
            if (maxIndexFiles > -1 && ++added >= maxIndexFiles) {
                break;
            }
        }
        tryToAdd(PipesIterator.COMPLETED_SEMAPHORE);
    }

    @Override
    public void initialize(Map<String, Param> params) throws TikaConfigException {
        if (fetcher == null) {
            fetcher = new BackoffHttpFetcher(ExtractorConfig.DEFAULT_THROTTLE_SECONDS);
        }
        if (fetcher instanceof Initializable) {
            ((Initializable) fetcher).initialize(params);
        }
        Matcher m = Pattern.compile("indexes/cdx-\\d{5,5}.gz\\Z").matcher("");
        if (configuredPaths.size() == 0) {
            try {
                loadLocalFiles(fetcher);
            } catch (IOException e) {
                throw new TikaConfigException("Problem reading from local directory");
            }
        }
        for (String p : configuredPaths) {
            if (p.endsWith("cc-index.paths.gz")) {
                try {
                    resolveIndexList(fetcher, p, resolvedIndexFiles);
                } catch (IOException | TikaException e) {
                    throw new TikaConfigException(e.getMessage());
                }
            } else if (m.reset(p).find()) {
                resolvedIndexFiles.add(p);
            } else {
                throw new TikaConfigException(
                        "Paths need to be path lists (.../cc-index.paths.gz) "
                                + "or indexes (indexes/cdx-\\d\\d\\d\\d\\d.gz");
            }
        }
    }

    private void loadLocalFiles(Fetcher fetcher) throws IOException {
        if (fetcher instanceof FileSystemFetcher) {
            Path basePath = ((FileSystemFetcher) fetcher).getBasePath();
            Files.walk(basePath)
                    .filter(p -> Files.isRegularFile(p))
                    .forEach(p -> configuredPaths.add(basePath.relativize(p).toString()));
        }
    }
}
