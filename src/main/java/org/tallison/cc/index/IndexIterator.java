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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.tallison.cc.index.extractor.ExtractorConfig;
import org.tallison.cc.index.io.TikaPipes;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.api.FetchEmitTuple;
import org.apache.tika.pipes.api.emitter.EmitKey;
import org.apache.tika.pipes.api.fetcher.FetchKey;
import org.apache.tika.pipes.api.fetcher.Fetcher;
import org.apache.tika.pipes.api.pipesiterator.PipesIterator;

public class IndexIterator {

    // temporary storage of the paths this class was constructed with.
    // During initialization, we figure out if these are index list paths
    // or literal index file paths.
    private final List<String> configuredPaths = new ArrayList<>();
    private final List<String> resolvedIndexFiles = new ArrayList<>();

    private final String profile;
    private final String basePath;
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
        this.profile = profile;
        this.basePath = basePath;
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

    /**
     * Returns a task that puts the resolved index files on the queue followed by one {@link
     * PipesIterator#COMPLETED_SEMAPHORE}; call after {@link #initialize()}.
     */
    public Callable<Long> newProducer(ArrayBlockingQueue<FetchEmitTuple> queue) {
        return () -> enqueue(queue);
    }

    private long enqueue(ArrayBlockingQueue<FetchEmitTuple> queue) throws InterruptedException {
        long added = 0;
        int skipped = 0;
        for (String p : resolvedIndexFiles) {
            if (skipped < skipIndexFiles) {
                skipped++;
                continue;
            }
            queue.put(new FetchEmitTuple(p, new FetchKey("", p), new EmitKey()));
            if (maxIndexFiles > -1 && ++added >= maxIndexFiles) {
                break;
            }
        }
        queue.put(PipesIterator.COMPLETED_SEMAPHORE);
        return added;
    }

    public void initialize() throws TikaConfigException {
        if (profile != null) {
            fetcher = TikaPipes.newCCS3Fetcher(profile, null);
        } else if (basePath != null) {
            fetcher = TikaPipes.newFileSystemFetcher(basePath);
        } else {
            fetcher = TikaPipes.newBackoffHttpFetcher(ExtractorConfig.DEFAULT_THROTTLE_SECONDS);
        }
        Matcher m = Pattern.compile("indexes/cdx-\\d{5,5}.gz\\Z").matcher("");
        if (configuredPaths.size() == 0) {
            try {
                loadLocalFiles();
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

    /** Returns the number of resolved index files after initialization. */
    public int getResolvedIndexFileCount() {
        return resolvedIndexFiles.size();
    }

    private void loadLocalFiles() throws IOException {
        if (profile == null && basePath != null) {
            Path base = Path.of(basePath);
            try (var paths = Files.walk(base)) {
                paths.filter(p -> Files.isRegularFile(p))
                        .forEach(p -> configuredPaths.add(base.relativize(p).toString()));
            }
        }
    }
}
