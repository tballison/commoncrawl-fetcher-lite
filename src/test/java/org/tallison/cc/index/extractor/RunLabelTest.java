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
package org.tallison.cc.index.extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class RunLabelTest {

    private static JsonNode json(String json) throws Exception {
        return new ObjectMapper().readTree(json);
    }

    @Test
    public void testExplicitRunLabelWins() throws Exception {
        JsonNode root = json("{\"runLabel\": \"My OneNote Run\", \"docs\": {\"path\": \"/data/pdfs\"}}");
        assertEquals("my_onenote_run", RunLabel.resolve(root));
    }

    @Test
    public void testDerivedFromDocsPath() throws Exception {
        JsonNode root = json("{\"docs\": {\"path\": \"/home/tallison/data/commoncrawl/onenote\"}}");
        assertEquals("onenote", RunLabel.resolve(root));
    }

    @Test
    public void testDerivedFromDocsPathTrailingSlash() throws Exception {
        JsonNode root = json("{\"docs\": {\"path\": \"/home/tallison/data/commoncrawl/onenote/\"}}");
        assertEquals("onenote", RunLabel.resolve(root));
    }

    @Test
    public void testDerivedFromBucketAndPrefix() throws Exception {
        JsonNode root = json("{\"docs\": {\"bucket\": \"my-bucket\", \"prefix\": \"some-docs\"}}");
        assertEquals("my-bucket-some-docs", RunLabel.resolve(root));
    }

    @Test
    public void testDerivedFromBucketOnly() throws Exception {
        JsonNode root = json("{\"docs\": {\"bucket\": \"my-bucket\"}}");
        assertEquals("my-bucket", RunLabel.resolve(root));
    }

    @Test
    public void testFallsBackToDefaultDocsPath() throws Exception {
        // matches ExtractorConfig.DEFAULT_FS_DOCS_PATH, the emitter's own fallback
        assertEquals("docs", RunLabel.resolve(json("{}")));
        assertEquals("docs", RunLabel.resolve(json("{\"docs\": {}}")));
    }

    @Test
    public void testSanitizesUnsafeCharacters() throws Exception {
        JsonNode root = json("{\"runLabel\": \"Office Docs! (v2)\"}");
        assertEquals("office_docs___v2_", RunLabel.resolve(root));
    }
}
