/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.cc.index.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.tallison.cc.index.extractor.ExtractorConfig;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.pipes.api.emitter.StreamEmitter;
import org.apache.tika.pipes.emitter.fs.FileSystemEmitter;
import org.apache.tika.pipes.emitter.s3.S3Emitter;
import org.apache.tika.pipes.fetcher.fs.FileSystemFetcher;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;
import org.apache.tika.plugins.ExtensionConfig;

/** Builds Tika 4.x fetchers/emitters directly from JSON, bypassing the plugin manager. */
public final class TikaPipes {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TikaPipes() {}

    public static S3Fetcher newCCS3Fetcher(String profile, long[] throttleSeconds)
            throws TikaConfigException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("credentialsProvider", "profile");
        m.put("profile", profile);
        m.put("bucket", ExtractorConfig.CC_S3_BUCKET);
        m.put("region", ExtractorConfig.CC_REGION);
        if (throttleSeconds != null) {
            m.put("throttleSeconds", throttleSeconds);
        }
        try {
            return S3Fetcher.build(config("s3Fetcher", "s3-fetcher", m));
        } catch (IOException e) {
            throw new TikaConfigException("couldn't build s3 fetcher", e);
        }
    }

    public static FileSystemFetcher newFileSystemFetcher(String basePath)
            throws TikaConfigException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("basePath", basePath);
        return new FileSystemFetcher(config("fsFetcher", "file-system-fetcher", m));
    }

    public static BackoffHttpFetcher newBackoffHttpFetcher(long[] throttleSeconds)
            throws TikaConfigException {
        return newBackoffHttpFetcher(throttleSeconds, RateLimitMonitor.global());
    }

    static BackoffHttpFetcher newBackoffHttpFetcher(
            long[] throttleSeconds, RateLimitMonitor monitor) throws TikaConfigException {
        ExtensionConfig ec = config("backoffHttpFetcher", "http-fetcher", Map.of());
        try {
            return new BackoffHttpFetcher(ec, HttpFetcher.build(ec), throttleSeconds, monitor);
        } catch (IOException e) {
            throw new TikaConfigException("couldn't build http fetcher", e);
        }
    }

    public static StreamEmitter newS3Emitter(
            String profile, String bucket, String region, String prefix)
            throws TikaConfigException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("credentialsProvider", "profile");
        m.put("profile", profile);
        m.put("bucket", bucket);
        m.put("region", region);
        if (prefix != null) {
            m.put("prefix", prefix);
        }
        m.put("fileExtension", "");
        try {
            return S3Emitter.build(config("s3Emitter", "s3-emitter", m));
        } catch (IOException e) {
            throw new TikaConfigException("couldn't build s3 emitter", e);
        }
    }

    public static StreamEmitter newFileSystemEmitter(String basePath) throws TikaConfigException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("basePath", basePath);
        m.put("onExists", "SKIP");
        m.put("fileExtension", "");
        try {
            return FileSystemEmitter.build(config("fsEmitter", "file-system-emitter", m));
        } catch (IOException e) {
            throw new TikaConfigException("couldn't build file system emitter", e);
        }
    }

    private static ExtensionConfig config(String id, String name, Map<String, Object> m)
            throws TikaConfigException {
        try {
            return new ExtensionConfig(id, name, MAPPER.writeValueAsString(m));
        } catch (JsonProcessingException e) {
            throw new TikaConfigException("couldn't serialize config", e);
        }
    }
}
