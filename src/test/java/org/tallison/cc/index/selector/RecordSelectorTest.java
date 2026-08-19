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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import org.tallison.cc.index.CCIndexRecord;

public class RecordSelectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CCIndexRecord makeRecord(String url, int status, String mime, String mimeDetected) {
        String row =
                "com,example)/ 20230101120000 "
                        + "{\"url\":\""
                        + url
                        + "\","
                        + "\"mime\":\""
                        + mime
                        + "\","
                        + "\"mime-detected\":\""
                        + mimeDetected
                        + "\","
                        + "\"status\":\""
                        + status
                        + "\"}";
        return CCIndexRecord.parseRecord(row).orElseThrow();
    }

    @Test
    public void testMustClauseAccepts() throws Exception {
        String json = "{\"must\":{\"status\":[{\"match\":\"200\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));
    }

    @Test
    public void testMustClauseRejects() throws Exception {
        String json = "{\"must\":{\"status\":[{\"match\":\"200\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 404, "text/html", "text/html");
        assertFalse(selector.select(record));
    }

    @Test
    public void testMustNotClauseRejects() throws Exception {
        String json = "{\"must_not\":{\"status\":[{\"match\":\"404\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 404, "text/html", "text/html");
        assertFalse(selector.select(record));
    }

    @Test
    public void testMustNotClauseAccepts() throws Exception {
        String json = "{\"must_not\":{\"status\":[{\"match\":\"404\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));
    }

    @Test
    public void testShouldClauseAtLeastOneMatch() throws Exception {
        String json =
                "{\"should\":{"
                        + "\"mime\":[{\"match\":\"text/html\"}],"
                        + "\"mime_detected\":[{\"match\":\"application/pdf\"}]"
                        + "}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        // mime matches, mime_detected does not
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/plain");
        assertTrue(selector.select(record));
    }

    @Test
    public void testShouldClauseNoneMatch() throws Exception {
        String json = "{\"should\":{" + "\"mime\":[{\"match\":\"application/pdf\"}]" + "}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertFalse(selector.select(record));
    }

    @Test
    public void testMustAndShouldCombined() throws Exception {
        String json =
                "{\"must\":{\"status\":[{\"match\":\"200\"}]},"
                        + "\"should\":{\"mime\":[{\"match\":\"text/html\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        // must passes, should passes
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));

        // must passes, should fails
        CCIndexRecord record2 =
                makeRecord("https://example.com/", 200, "application/pdf", "application/pdf");
        assertFalse(selector.select(record2));

        // must fails, should passes
        CCIndexRecord record3 = makeRecord("https://example.com/", 404, "text/html", "text/html");
        assertFalse(selector.select(record3));
    }

    @Test
    public void testEmptyShouldMustOnlyPasses() throws Exception {
        String json = "{\"must\":{\"status\":[{\"match\":\"200\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));
    }

    @Test
    public void testNullFieldMustClauseRejects() throws Exception {
        // truncated field is null by default
        String json = "{\"must\":{\"truncated\":[{\"match\":\"length\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertFalse(selector.select(record));
    }

    @Test
    public void testNullFieldMustNotSkips() throws Exception {
        // truncated field is null, must_not should skip it (continue)
        String json = "{\"must_not\":{\"truncated\":[{\"match\":\"length\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));
    }

    @Test
    public void testMatchSelectorCaseInsensitive() throws Exception {
        String json =
                "{\"must\":{\"mime\":[{\"match\":\"TEXT/HTML\"," + "\"case_sensitive\":false}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));
    }

    @Test
    public void testRegexSelector() throws Exception {
        String json = "{\"must\":{\"mime\":[{\"pattern\":\"text/.*\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));

        CCIndexRecord record2 =
                makeRecord("https://example.com/", 200, "application/pdf", "application/pdf");
        assertFalse(selector.select(record2));
    }

    @Test
    public void testExtensionsSelector() throws Exception {
        String json = "{\"should\":{\"url\":[{\"extensions\":\"pdf,doc\"}]}}";
        RecordSelector selector = MAPPER.readValue(json, RecordSelector.class);
        CCIndexRecord record =
                makeRecord(
                        "https://example.com/file.pdf", 200, "application/pdf", "application/pdf");
        assertTrue(selector.select(record));

        CCIndexRecord record2 =
                makeRecord("https://example.com/file.txt", 200, "text/plain", "text/plain");
        assertFalse(selector.select(record2));
    }

    @Test
    public void testAcceptAllRecords() {
        RecordSelector selector = RecordSelector.ACCEPT_ALL_RECORDS;
        CCIndexRecord record = makeRecord("https://example.com/", 200, "text/html", "text/html");
        assertTrue(selector.select(record));
    }
}
