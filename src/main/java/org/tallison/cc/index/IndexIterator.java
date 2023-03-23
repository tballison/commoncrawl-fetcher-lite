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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.tallison.cc.index.fetcher.FetcherConfig;
import org.tallison.cc.index.io.BackoffHttpFetcher;

import org.apache.tika.config.Initializable;
import org.apache.tika.config.Param;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.fs.FileSystemFetcher;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

public class IndexIterator extends PipesIterator implements Initializable {

    //temporary storage of the paths this class was constructed with
    //during initialization, we'll figure out if these are literal index paths
    //or paths to index lists.
    private final List<String> initPaths = new ArrayList<>();
    private final List<String> indexPaths = new ArrayList<>();

    private Fetcher fetcher = null;

    @JsonCreator
    public IndexIterator(@JsonProperty("profile") String profile,
                         @JsonProperty("basePath") String basePath,
                         @JsonProperty("paths") List<String> indexPaths) {
        if (profile != null) {
            fetcher = new S3Fetcher();
            ((S3Fetcher) fetcher).setProfile(profile);
            ((S3Fetcher) fetcher).setCredentialsProvider("profile");
            ((S3Fetcher) fetcher).setBucket(FetcherConfig.CC_S3_BUCKET);
            ((S3Fetcher) fetcher).setRegion(FetcherConfig.CC_REGION);
        } else if (basePath != null) {
            fetcher = new FileSystemFetcher();
            ((FileSystemFetcher) fetcher).setBasePath(basePath);
        } else {
            //do nothing
        }
        initPaths.addAll(indexPaths);
    }

    private static void addIndexPaths(Fetcher fetcher, String path, List<String> indexPaths)
            throws IOException, TikaException {

        try (InputStream is = fetcher.fetch(path, new Metadata())) {
            try (BufferedReader reader = getReader(is, path)) {
                String line = reader.readLine();
                while (line != null) {
                    if (line.startsWith("#") || !line.endsWith(".gz")) {
                        //skip comments and paths for index files that do not end in .gz
                        line = reader.readLine();
                    }
                    indexPaths.add(line);
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
        for (String p : indexPaths) {
            FetchEmitTuple t = new FetchEmitTuple(p, new FetchKey("", p), new EmitKey());
            tryToAdd(t);
        }
        tryToAdd(PipesIterator.COMPLETED_SEMAPHORE);
    }

    @Override
    public void initialize(Map<String, Param> params) throws TikaConfigException {
        if (fetcher == null) {
            // TODO -- figure out if we actually need to fetch (e.g. to fetch cc-index.paths.gz) or
            //if these are just the literal paths to the index files
            fetcher = new BackoffHttpFetcher(new long[]{30, 120});
        }
        if (fetcher instanceof Initializable) {
            ((Initializable) fetcher).initialize(params);
        }
        Matcher m = Pattern.compile("indexes/cdx-\\d{5,5}.gz\\Z").matcher("");
        for (String p : initPaths) {
            if (p.endsWith("cc-index.paths.gz")) {
                try {
                    addIndexPaths(fetcher, p, indexPaths);
                } catch (IOException | TikaException e) {
                    throw new TikaConfigException(e.getMessage());
                }
            } else if (m.reset(p).find()) {
                indexPaths.add(p);
            } else {
                throw new TikaConfigException(
                        "Paths need to be path lists (.../cc-index.paths.gz) " +
                                "or indexes (indexes/cdx-\\d\\d\\d\\d\\d.gz");
            }
        }

    }
}
