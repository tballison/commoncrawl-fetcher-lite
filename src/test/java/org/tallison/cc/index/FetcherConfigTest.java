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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.tallison.cc.index.fetcher.FetcherConfig;
import org.tallison.cc.index.io.BackoffHttpFetcher;

import org.apache.tika.pipes.emitter.fs.FileSystemEmitter;
import org.apache.tika.pipes.emitter.s3.S3Emitter;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;

public class FetcherConfigTest {

    @Test
    public void testBasic() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic-http.json").toURI());
        FetcherConfig fetcherConfig = new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        assertEquals(BackoffHttpFetcher.class, fetcherConfig.newFetcher().getClass());
        assertEquals(FileSystemEmitter.class, fetcherConfig.newEmitter().getClass());
    }

    @Test
    public void testLocalIndices() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic-local.json").toURI());
        FetcherConfig fetcherConfig = new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        //TODO -- add actual unit test that tests FSFetcher
        assertEquals(BackoffHttpFetcher.class, fetcherConfig.newFetcher().getClass());
        assertEquals(FileSystemEmitter.class, fetcherConfig.newEmitter().getClass());
    }

    @Test
    public void testS3() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic-s3.json").toURI());
        FetcherConfig fetcherConfig = new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        //TODO -- add actual unit test that tests fetcher and emitter
        assertEquals(S3Fetcher.class, fetcherConfig.newFetcher().getClass());
        assertEquals(S3Emitter.class, fetcherConfig.newEmitter().getClass());
    }
}
