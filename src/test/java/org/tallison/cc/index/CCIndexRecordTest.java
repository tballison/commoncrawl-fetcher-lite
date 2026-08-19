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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import java.util.Optional;

public class CCIndexRecordTest {

    @Test
    public void testParseRecordValid() {
        String row =
                "com,example)/ 20230101120000 "
                        + "{\"url\":\"https://example.com/\","
                        + "\"mime\":\"text/html\","
                        + "\"mime-detected\":\"text/html\","
                        + "\"status\":\"200\","
                        + "\"digest\":\"ABC123\","
                        + "\"length\":\"1234\","
                        + "\"offset\":\"5678\","
                        + "\"filename\":\"crawl-data/segment/warc.gz\"}";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertTrue(result.isPresent());
        CCIndexRecord record = result.get();
        assertEquals("https://example.com/", record.getUrl());
        assertEquals("text/html", record.getMime());
        assertEquals("text/html", record.getMimeDetected());
        assertEquals(200, record.getStatus());
        assertEquals("ABC123", record.getDigest());
        assertEquals(1234L, record.getLength());
        assertEquals(5678, record.getOffset());
        assertEquals("crawl-data/segment/warc.gz", record.getFilename());
    }

    @Test
    public void testParseRecordMalformedJsonRepaired() {
        // Extra characters after closing brace -- tryRepair should fix this
        String row =
                "com,example)/ 20230101120000 "
                        + "{\"url\":\"https://example.com/\","
                        + "\"status\":\"200\","
                        + "\"mime\":\"text/html\"} some extra garbage";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertTrue(result.isPresent());
        assertEquals("https://example.com/", result.get().getUrl());
        assertEquals(200, result.get().getStatus());
    }

    @Test
    public void testParseRecordCompletelyInvalid() {
        String row = "com,example)/ 20230101120000 not-json-at-all";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertFalse(result.isPresent());
    }

    @Test
    public void testParseRecordMissingDate() {
        // Only one space, so dateI will be < 0
        String row = "singletoken";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetTldNormalUrl() {
        assertEquals("com", CCIndexRecord.getTLD("https://example.com/path"));
    }

    @Test
    public void testGetTldIpAddress() {
        assertEquals("", CCIndexRecord.getTLD("http://192.168.1.1/path"));
    }

    @Test
    public void testGetTldNull() {
        assertEquals("", CCIndexRecord.getTLD(null));
    }

    @Test
    public void testGetTldBadUri() {
        assertEquals("", CCIndexRecord.getTLD("://not a valid uri"));
    }

    @Test
    public void testNormalizeMimeStripsAndLowercases() {
        assertEquals("text/html", CCIndexRecord.normalizeMime("\"TEXT/HTML\""));
        assertEquals("text/html", CCIndexRecord.normalizeMime("  TEXT/HTML  "));
        assertEquals("application/pdf", CCIndexRecord.normalizeMime("\"Application/PDF\""));
    }

    @Test
    public void testNormalizeMimeNull() {
        assertNull(CCIndexRecord.normalizeMime(null));
    }

    @Test
    public void testGetHostValid() {
        String row =
                "com,example)/ 20230101120000 "
                        + "{\"url\":\"https://www.example.com/page\","
                        + "\"status\":\"200\"}";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertTrue(result.isPresent());
        assertEquals("www.example.com", result.get().getHost());
    }

    @Test
    public void testGetHostMalformedUrl() {
        String row =
                "com,example)/ 20230101120000 " + "{\"url\":\"not a url\"," + "\"status\":\"200\"}";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertTrue(result.isPresent());
        assertEquals("", result.get().getHost());
    }

    @Test
    public void testGetOffsetHeader() {
        String row =
                "com,example)/ 20230101120000 "
                        + "{\"url\":\"https://example.com/\","
                        + "\"length\":\"100\","
                        + "\"offset\":\"500\"}";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertTrue(result.isPresent());
        assertEquals("bytes=500-599", result.get().getOffsetHeader());
    }

    @Test
    public void testGetTldMultiPartDomain() {
        assertEquals("uk", CCIndexRecord.getTLD("https://example.co.uk/path"));
    }

    @Test
    public void testParseRecordUnknownFieldIgnored() {
        // CC-MAIN-2026-30 added a "recordid" field this class doesn't declare; without
        // @JsonIgnoreProperties(ignoreUnknown = true) every such record fails to parse.
        String row =
                "com,example)/ 20230101120000 "
                        + "{\"url\":\"https://example.com/\","
                        + "\"status\":\"200\","
                        + "\"mime\":\"text/html\","
                        + "\"recordid\":\"019f64a3-b95c-7cfd-8086-1ff030ac68b5\"}";
        Optional<CCIndexRecord> result = CCIndexRecord.parseRecord(row);
        assertTrue(result.isPresent());
        assertEquals("https://example.com/", result.get().getUrl());
        assertEquals(200, result.get().getStatus());
    }
}
