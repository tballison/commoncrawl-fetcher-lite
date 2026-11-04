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
package org.tallison.cc.index.selector;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tallison.cc.index.CCIndexRecord;

public class RecordSelector {

    private static Logger LOGGER = LoggerFactory.getLogger(RecordSelector.class);

    public static RecordSelector ACCEPT_ALL_RECORDS = new AcceptAllRecords();
    @JsonProperty Map<String, List<SelectorClause>> must = new HashMap<>();
    @JsonProperty Map<String, List<SelectorClause>> must_not = new HashMap<>();
    @JsonProperty Map<String, List<SelectorClause>> should = new HashMap<>();

    public boolean select(CCIndexRecord record) {

        for (Map.Entry<String, List<SelectorClause>> e : must_not.entrySet()) {
            String val = getStringValue(e.getKey(), record);
            for (SelectorClause clause : e.getValue()) {
                if (val == null && !clause.evaluatesNull()) {
                    LOGGER.debug("Value is null for '{}' in the must not clause", e.getKey());
                    continue;
                }
                if (clause.select(val)) {
                    return false;
                }
            }
        }

        for (Map.Entry<String, List<SelectorClause>> e : must.entrySet()) {
            String val = getStringValue(e.getKey(), record);
            for (SelectorClause clause : e.getValue()) {
                if (val == null && !clause.evaluatesNull()) {
                    LOGGER.debug(
                            "Value is null for '{}' in the must clause. Record not selected.",
                            e.getKey());
                    return false;
                }
                if (!clause.select(val)) {
                    return false;
                }
            }
        }
        if (should.size() == 0) {
            return true;
        }
        for (Map.Entry<String, List<SelectorClause>> e : should.entrySet()) {
            String val = getStringValue(e.getKey(), record);
            for (SelectorClause clause : e.getValue()) {
                if (val == null && !clause.evaluatesNull()) {
                    continue;
                }
                if (clause.select(val)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Seeds every sampling clause (clause i gets seed + i, in config order) so that sampling is
     * reproducible for a single-threaded run over the same input order.
     */
    public void applySeed(long seed) {
        int i = 0;
        for (Map<String, List<SelectorClause>> m : List.of(must_not, must, should)) {
            for (List<SelectorClause> clauses : m.values()) {
                for (SelectorClause clause : clauses) {
                    if (clause instanceof AbstractSamplingSelector) {
                        ((AbstractSamplingSelector) clause).reseed(seed + i++);
                    }
                }
            }
        }
    }

    private String getStringValue(String key, CCIndexRecord record) {

        switch (key) {
            case "mime_detected":
                return record.getMimeDetected();
            case "truncated":
                return record.getTruncated();
            case "mime":
                return record.getMime();
            case "status":
                return record.getStatus() == null ? null : Integer.toString(record.getStatus());
            case "url":
                return record.getUrl();
            case "host":
                return record.getHost();
            case "digest":
                return record.getDigest();
            case "charset":
                // Index charset field; provenance (header vs. meta vs. detector) unverified.
                return record.getCharset();
            case "languages":
                // Comma-separated; assumed CLD2 output, not ground truth.
                return record.getLanguages();
            case "tld":
                return CCIndexRecord.getTLD(record.getUrl());
            default:
                throw new IllegalArgumentException("Don't yet support key " + key);
        }
    }
}
