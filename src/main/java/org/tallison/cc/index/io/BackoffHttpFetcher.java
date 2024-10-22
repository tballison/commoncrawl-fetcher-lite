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
package org.tallison.cc.index.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.cc.index.extractor.ExtractorConfig;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;

/**
 * We need this because it allows for back-off on 503
 * and it adds the protocol+host base to the paths: {@link ExtractorConfig#CC_HTTPS_BASE}
 */
public class BackoffHttpFetcher extends HttpFetcher {

    private static Logger LOGGER = LoggerFactory.getLogger(BackoffHttpFetcher.class);

    //backoff
    private final long[] throttleSeconds;

    public BackoffHttpFetcher(long[] throttleSeconds) {
        this.throttleSeconds = throttleSeconds;
    }

    @Override
    public InputStream fetch(String fetchKey, Metadata metadata, ParseContext parseContext)
            throws TikaException, IOException {
        return fetchWithBackOff(new FetchKey("name", getUrl(fetchKey)), metadata, parseContext);
    }

    @Override
    public InputStream fetch(String fetchkey, long rangeStart, long rangeEnd, Metadata metadata)
            throws IOException {
        return fetchWithBackOff(new FetchKey("name", getUrl(fetchkey), rangeStart, rangeEnd),
                metadata, new ParseContext());
    }

    private String getUrl(String fetchKey) {
        if (!fetchKey.startsWith("http")) {
            if (fetchKey.startsWith("/")) {
                return ExtractorConfig.CC_HTTPS_BASE + fetchKey;
            } else {
                return ExtractorConfig.CC_HTTPS_BASE + "/" + fetchKey;
            }
        }
        return fetchKey;
    }

    private InputStream fetchWithBackOff(FetchKey fetchKey, Metadata metadata,
                                         ParseContext parseContext) throws IOException {
        int tries = 0;
        while (tries < throttleSeconds.length) {
            try {
                return _fetch(fetchKey, metadata);
            } catch (IOException e) {
                if (e.getMessage() == null) {
                    throw e;
                }
                Matcher m = Pattern.compile("bad status code: (\\d+)").matcher(e.getMessage());
                if (m.find() && m.group(1).equals("503")) {
                    long sleepMs = 1000 * throttleSeconds[tries];
                    LOGGER.warn("got backoff warning (#{}) for {}. Will sleep {} seconds. " +
                                    "Message: {}. ",
                            tries + 1, fetchKey.getFetchKey(),
                            throttleSeconds[tries],
                            e.getMessage());
                    //sleep, back off
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    throw e;
                }
            } catch (TikaException e) {
                throw new IOException(e);
            }
            tries++;
        }
        throw new ThrottleException();
    }

    private TikaInputStream _fetch(FetchKey fetchKey, Metadata metadata)
            throws IOException, TikaException {
        if (fetchKey.getRangeStart() > 0) {
            return (TikaInputStream) super.fetch(fetchKey.getFetchKey(), fetchKey.getRangeStart(),
                    fetchKey.getRangeEnd(), metadata);
        } else {
            return (TikaInputStream) super.fetch(fetchKey.getFetchKey(), metadata, new ParseContext());
        }

    }

    @Override
    public String getName() {
        return "backoffHttpFetcher";
    }

}
