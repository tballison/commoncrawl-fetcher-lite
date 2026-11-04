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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.tallison.cc.index.CCIndexRecord;

public class CharsetSelectionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static CCIndexRecord rec(
            String url, String mimeDetected, String charset, String languages) {
        StringBuilder json = new StringBuilder("{\"url\":\"" + url + "\",\"status\":\"200\"");
        json.append(",\"mime-detected\":\"").append(mimeDetected).append("\"");
        if (charset != null) {
            json.append(",\"charset\":\"").append(charset).append("\"");
        }
        if (languages != null) {
            json.append(",\"languages\":\"").append(languages).append("\"");
        }
        json.append("}");
        Optional<CCIndexRecord> r = CCIndexRecord.parseRecord("x 0 " + json);
        return r.orElseThrow();
    }

    private static RecordSelector recipe(String name) throws Exception {
        JsonNode root = MAPPER.readTree(new File("examples/recipes/" + name));
        return MAPPER.treeToValue(root.get("recordSelector"), RecordSelector.class);
    }

    @Test
    public void testRecipesAcceptHtmlAndXhtml() throws Exception {
        for (String[] c :
                new String[][] {
                    {"charset-cyrillic-non-utf8.json", "http://a.ru/", "windows-1251"},
                    {"charset-cjk-non-utf8.json", "http://a.jp/", "Shift_JIS"},
                    {"charset-long-tail.json", "http://a.com/", "KOI8-R"}
                }) {
            RecordSelector s = recipe(c[0]);
            assertTrue(s.select(rec(c[1], "text/html", c[2], null)), c[0] + " html");
            assertTrue(s.select(rec(c[1], "application/xhtml+xml", c[2], null)), c[0] + " xhtml");
            assertFalse(s.select(rec(c[1], "text/plain", c[2], null)), c[0] + " plain");
        }
    }

    @Test
    public void testDeclarationAbsentRecipe() throws Exception {
        RecordSelector s = recipe("charset-declaration-absent.json");
        assertTrue(s.select(rec("http://a.com/", "text/html", null, null)));
        assertTrue(s.select(rec("http://a.com/", "application/xhtml+xml", "", null)));
        assertFalse(s.select(rec("http://a.com/", "text/html", "UTF-8", null)));
        assertFalse(s.select(rec("http://a.com/", "text/plain", null, null)));
    }

    @Test
    public void testAbsentClause() throws Exception {
        RecordSelector absent =
                MAPPER.readValue(
                        "{\"must\":{\"languages\":[{\"absent\":true}]}}", RecordSelector.class);
        assertTrue(absent.select(rec("http://a.com/", "text/html", null, null)));
        assertTrue(absent.select(rec("http://a.com/", "text/html", null, " ")));
        assertFalse(absent.select(rec("http://a.com/", "text/html", null, "eng")));

        RecordSelector present =
                MAPPER.readValue(
                        "{\"must\":{\"languages\":[{\"absent\":false}]}}", RecordSelector.class);
        assertFalse(present.select(rec("http://a.com/", "text/html", null, null)));
        assertTrue(present.select(rec("http://a.com/", "text/html", null, "eng")));

        RecordSelector notAbsent =
                MAPPER.readValue(
                        "{\"must_not\":{\"languages\":[{\"absent\":true}]}}", RecordSelector.class);
        assertFalse(notAbsent.select(rec("http://a.com/", "text/html", null, null)));
        assertTrue(notAbsent.select(rec("http://a.com/", "text/html", null, "eng")));

        RecordSelector should =
                MAPPER.readValue(
                        "{\"should\":{\"languages\":[{\"absent\":true}]}}", RecordSelector.class);
        assertTrue(should.select(rec("http://a.com/", "text/html", null, null)));
        assertFalse(should.select(rec("http://a.com/", "text/html", null, "eng")));
    }

    @Test
    public void testMatchOnNullStillRejects() throws Exception {
        RecordSelector s =
                MAPPER.readValue(
                        "{\"must\":{\"languages\":[{\"match\":\"eng\"}]}}", RecordSelector.class);
        assertFalse(s.select(rec("http://a.com/", "text/html", null, null)));
    }

    private static List<Boolean> run(RecordSelector s, int n) {
        List<Boolean> out = new ArrayList<>();
        CCIndexRecord r = rec("http://a.com/", "text/html", null, null);
        for (int i = 0; i < n; i++) {
            out.add(s.select(r));
        }
        return out;
    }

    @Test
    public void testSeededSamplingReproducible() throws Exception {
        String json = "{\"must\":{\"mime_detected\":[{\"match\":\"text/html\",\"sample\":0.5}]}}";
        RecordSelector a = MAPPER.readValue(json, RecordSelector.class);
        RecordSelector b = MAPPER.readValue(json, RecordSelector.class);
        RecordSelector c = MAPPER.readValue(json, RecordSelector.class);
        a.applySeed(42);
        b.applySeed(42);
        c.applySeed(43);
        List<Boolean> ra = run(a, 200);
        assertEquals(ra, run(b, 200));
        assertTrue(ra.contains(true) && ra.contains(false));
        assertFalse(ra.equals(run(c, 200)));
    }
}
