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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MatchSelectorTest {

    @Test
    public void testExactMatchCaseSensitive() {
        MatchSelector selector = new MatchSelector("text/html", null, null);
        assertTrue(selector.select("text/html"));
    }

    @Test
    public void testExactMatchCaseSensitiveFails() {
        MatchSelector selector = new MatchSelector("text/html", null, null);
        assertFalse(selector.select("TEXT/HTML"));
    }

    @Test
    public void testCaseInsensitiveMatch() {
        MatchSelector selector = new MatchSelector("text/html", null, false);
        assertTrue(selector.select("TEXT/HTML"));
        assertTrue(selector.select("Text/Html"));
        assertTrue(selector.select("text/html"));
    }

    @Test
    public void testNoMatch() {
        MatchSelector selector = new MatchSelector("text/html", null, null);
        assertFalse(selector.select("application/pdf"));
    }

    @Test
    public void testSampleOneAlwaysSelects() {
        MatchSelector selector = new MatchSelector("text/html", 1.0, null);
        // With sample=1.0, every match should be selected
        for (int i = 0; i < 100; i++) {
            assertTrue(selector.select("text/html"));
        }
    }

    @Test
    public void testSampleZeroAlwaysRejects() {
        MatchSelector selector = new MatchSelector("text/html", 0.0, null);
        // With sample=0.0, even a match should be rejected
        for (int i = 0; i < 100; i++) {
            assertFalse(selector.select("text/html"));
        }
    }

    @Test
    public void testSampleNullDefaultsToAll() {
        MatchSelector selector = new MatchSelector("200", null, null);
        // null sample means SampleAll, so all matches should be selected
        for (int i = 0; i < 100; i++) {
            assertTrue(selector.select("200"));
        }
    }
}
