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
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.fetcher.FetchKey;
import org.apache.tika.pipes.fetcher.http.HttpFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPFetchWrapper {


    private static Logger LOGGER = LoggerFactory.getLogger(HTTPFetchWrapper.class);

    private HttpFetcher fetcher = new HttpFetcher();
    private int maxTriesOn503 = 3;

    //backoff
    private long[] throttleSeconds = new long[] {30, 120, 600};

    public HTTPFetchWrapper() throws TikaConfigException {
        fetcher.initialize(Collections.EMPTY_MAP);
    }

    public void setThrottleStepsInSeconds(long[] throttleSeconds) {
        this.throttleSeconds = throttleSeconds;
    }

    public TikaInputStream openStream(String url)
            throws IOException, InterruptedException, TikaException {
        FetchEmitTuple fetchEmitTuple = new FetchEmitTuple(
                url,
                new FetchKey("", url),
                new EmitKey()
        );
        return fetch(fetchEmitTuple);
    }

    public TikaInputStream fetch(FetchEmitTuple t)
            throws IOException, InterruptedException, TikaException {
        int tries = 0;
        while (tries < throttleSeconds.length) {
            try {
                return _fetch(t);
            } catch (IOException e) {
                if (e.getMessage() == null) {
                    throw e;
                }
                Matcher m = Pattern.compile("bad status code: (\\d+)").matcher(e.getMessage());
                if (m.find() && m.group(1).equals("503")) {
                    long sleepMs = 1000 * throttleSeconds[tries];
                    LOGGER.warn("got backoff warning (#{}): {}. Will sleep {} seconds",
                            tries + 1, e.getMessage(), throttleSeconds);
                    //sleep, back off
                    Thread.sleep(sleepMs);
                } else {
                    throw e;
                }
            }
            tries++;
        }
        throw new ThrottleException();
    }

    private TikaInputStream _fetch(FetchEmitTuple t) throws IOException, TikaException {
        FetchKey fetchKey = t.getFetchKey();
        Metadata metadata = new Metadata();
        if (fetchKey.getRangeStart() > 0) {
            return (TikaInputStream) fetcher.fetch(fetchKey.getFetchKey(), fetchKey.getRangeStart(),
                    fetchKey.getRangeEnd(), metadata);
        } else {
            return (TikaInputStream) fetcher.fetch(fetchKey.getFetchKey(), metadata);
        }

    }
}
