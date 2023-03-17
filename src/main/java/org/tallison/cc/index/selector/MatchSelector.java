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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * match exact string
 */
public class MatchSelector extends AbstractSamplingSelector {

    private static final boolean DEFAULT_CASE_SENSITIVE = true;
    final boolean caseSensitive;
    final String match;

    @JsonCreator
    MatchSelector(@JsonProperty("match") String match,
                  @JsonProperty("sample") Double sample,
                  @JsonProperty("case_sensitive") Boolean caseSensitive) {
        super(sample == null ? new SampleAll() : new SampleSome(sample));
        this.match = match;
        this.caseSensitive = caseSensitive == null ? DEFAULT_CASE_SENSITIVE : caseSensitive;
    }

    @Override
    public boolean select(String val) {
        if (caseSensitive) {
            if (match.equals(val)) {
                return true;
            }
        } else {
            if (match.equalsIgnoreCase(val)) {
                return true;
            }
        }
        return false;
    }
}
