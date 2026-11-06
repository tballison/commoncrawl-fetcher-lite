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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcCompression;
import org.netpreserve.jwarc.WarcResponse;
import org.netpreserve.jwarc.WarcWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.CCIndexRecord;
import org.tallison.cc.index.io.RateLimitMonitor;

public class ProvenanceAndManifestTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static byte[] warcGz(byte[] body) throws Exception {
        String http =
                "HTTP/1.1 200 OK\r\n"
                        + "Content-Type: text/html; charset=windows-1251\r\n"
                        + "Content-Language: ru\r\n"
                        + "Set-Cookie: a=1\r\n"
                        + "Set-Cookie: b=2\r\n\r\n";
        ByteArrayOutputStream http2 = new ByteArrayOutputStream();
        http2.write(http.getBytes(StandardCharsets.ISO_8859_1));
        http2.write(body);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (WarcWriter w = new WarcWriter(Channels.newChannel(out), WarcCompression.GZIP)) {
            w.write(
                    new WarcResponse.Builder("http://example.ru/")
                            .body(MediaType.HTTP_RESPONSE, http2.toByteArray())
                            .build());
        }
        return out.toByteArray();
    }

    private static ExtractorConfig config(Path dir, String extra) throws Exception {
        String json =
                "{\"docs\":{\"path\":\""
                        + dir.resolve("docs").toString().replace("\\", "/")
                        + "\"},\"targetPathPattern\":\"xx/xx/xxx\","
                        + "\"provenancePath\":\""
                        + dir.resolve("prov.jsonl").toString().replace("\\", "/")
                        + "\","
                        + "\"manifestPath\":\""
                        + dir.resolve("manifest.json").toString().replace("\\", "/")
                        + "\""
                        + extra
                        + "}";
        File f = dir.resolve("config.json").toFile();
        Files.writeString(f.toPath(), json);
        return ExtractorConfig.load(f);
    }

    @Test
    public void testProvenanceLineHasAllFields(@TempDir Path dir) throws Exception {
        ExtractorConfig config = config(dir, "");
        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        FileFromCCWarcExtractor extractor = new FileFromCCWarcExtractor(config, counter);
        CCIndexRecord r =
                CCIndexRecord.parseRecord(
                                "x 0 {\"url\":\"http://example.ru/\",\"mime\":\"text/html\","
                                        + "\"mime-detected\":\"text/html\",\"status\":\"200\","
                                        + "\"digest\":\"D\",\"length\":\"100\",\"offset\":\"7\","
                                        + "\"filename\":\"crawl-data/CC-MAIN-2025-08/segments/s/warc/w.warc.gz\","
                                        + "\"charset\":\"windows-1251\"}")
                        .orElseThrow();
        extractor.parseWarc(r.getUrl(), r, warcGz(new byte[] {(byte) 0xCF, (byte) 0xF0, 'x'}));
        config.getProvenanceWriter().close();

        List<String> lines = Files.readAllLines(dir.resolve("prov.jsonl"));
        assertEquals(1, lines.size());
        JsonNode n = MAPPER.readTree(lines.get(0));
        assertEquals("http://example.ru/", n.get("url").asText());
        assertEquals("text/html", n.get("mime_detected").asText());
        assertEquals(200, n.get("status").asInt());
        assertEquals("windows-1251", n.get("charset").asText());
        assertTrue(n.get("languages").isNull());
        assertEquals("CC-MAIN-2025-08", n.get("crawl_id").asText());
        assertEquals(3, n.get("stored_length").asInt());
        assertEquals(64, n.get("sha256").asText().length());
        assertNotNull(n.get("output_path").asText());
        JsonNode h = n.get("http_headers");
        assertEquals("text/html; charset=windows-1251", h.get("Content-Type").get(0).asText());
        assertEquals("ru", h.get("Content-Language").get(0).asText());
        assertEquals(2, h.get("Set-Cookie").size());
        assertTrue(Files.exists(dir.resolve("docs")));
        assertEquals(1, counter.getFilesWritten().get());
        assertEquals(1L, counter.getWrittenByStratum().get("text/html|windows-1251"));
    }

    @Test
    public void testManifestStartAndEnd(@TempDir Path dir) throws Exception {
        ExtractorConfig config = config(dir, ",\"seed\":42");
        assertEquals(42L, config.getSeed());
        RunManifest m = RunManifest.start(config, "Fetch");
        JsonNode start = MAPPER.readTree(dir.resolve("manifest.json").toFile());
        assertEquals("running", start.get("status").asText());
        assertTrue(start.get("ended").isNull());
        assertEquals(42, start.get("seed").asInt());
        assertEquals("xx/xx/xxx", start.get("config").get("targetPathPattern").asText());

        CCIndexReaderCounter counter = new CCIndexReaderCounter();
        counter.getRecordsRead().set(10);
        counter.recordWritten("text/html|utf-8");
        m.finish(counter, "completed");
        JsonNode end = MAPPER.readTree(dir.resolve("manifest.json").toFile());
        assertEquals("completed", end.get("status").asText());
        assertEquals(start.get("started").asText(), end.get("started").asText());
        assertTrue(!end.get("ended").isNull());
        assertEquals(10, end.get("counts").get("records_read").asInt());
        assertEquals(1, end.get("counts").get("files_written").asInt());
        assertEquals(
                1,
                end.get("counts")
                        .get("written_by_mime_detected_and_charset")
                        .get("text/html|utf-8")
                        .asInt());
        String commit = end.get("tool_git_commit").asText();
        assertTrue(commit.equals("unknown") || commit.matches("[0-9a-f]{40}"), commit);
    }

    @Test
    public void testManifestRateLimitCounts(@TempDir Path dir) throws Exception {
        RateLimitMonitor monitor = new RateLimitMonitor(() -> 0L, 5, 60_000, 600_000);
        monitor.record("data.commoncrawl.org", 503);
        monitor.record("data.commoncrawl.org", 429);
        RunManifest m = new RunManifest(config(dir, ""), "QueryIndex", "unknown", monitor);
        m.setDuckDbHttp(100, Map.of(503, 7L));
        m.finish(new CCIndexReaderCounter(), "completed");
        JsonNode rl = MAPPER.readTree(dir.resolve("manifest.json").toFile()).get("rate_limited");
        assertEquals(2, rl.get("http_total").asInt());
        assertEquals(1, rl.get("http_by_status").get("503").asInt());
        assertEquals(1, rl.get("http_by_status").get("429").asInt());
        assertEquals(0, rl.get("pauses").asInt());
        assertEquals(100, rl.get("duckdb_requests").asInt());
        assertEquals(7, rl.get("duckdb_by_status").get("503").asInt());
    }

    @Test
    public void testSeedGeneratedWhenAbsent(@TempDir Path dir) throws Exception {
        ExtractorConfig config = config(dir, "");
        assertNotNull(config.getSeed());
        assertTrue(config.isSeedGenerated());
        assertTrue(!config(dir, ",\"seed\":7").isSeedGenerated());
        Optional<String> unused = Optional.empty();
        assertTrue(unused.isEmpty());
    }
}
