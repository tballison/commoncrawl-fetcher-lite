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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.tallison.cc.index.CCIndexReaderCounter;
import org.tallison.cc.index.io.RateLimitMonitor;

/** Run manifest JSON: written at start, rewritten at end. */
public class RunManifest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunManifest.class);
    private static final ObjectMapper MAPPER =
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final ExtractorConfig config;
    private final String command;
    private final String commit;
    private final Instant started;
    private final Path path;
    private final RateLimitMonitor rateLimits;
    private long duckDbRequests = -1;
    private Map<Integer, Long> duckDbRateLimited = Map.of();

    private RunManifest(ExtractorConfig config, String command, String commit) {
        this(config, command, commit, RateLimitMonitor.global());
    }

    RunManifest(
            ExtractorConfig config, String command, String commit, RateLimitMonitor rateLimits) {
        this.rateLimits = rateLimits;
        this.config = config;
        this.command = command;
        this.commit = commit;
        this.started = Instant.now();
        this.path = config.getManifestPath();
    }

    public static RunManifest start(ExtractorConfig config, String command) {
        RunManifest m = new RunManifest(config, command, gitCommit());
        m.write(null, null, "running");
        return m;
    }

    public void finish(CCIndexReaderCounter counter, String status) {
        if (rateLimits.getTotal() > 0) {
            LOGGER.warn("Common Crawl HTTP {}", rateLimits);
        } else {
            LOGGER.info("Common Crawl HTTP: no 503/429 responses");
        }
        write(Instant.now(), counter, status);
    }

    /** DuckDB's HTTP request count and its 503/429 retries by status, for the manifest. */
    void setDuckDbHttp(long requests, Map<Integer, Long> rateLimitedByStatus) {
        this.duckDbRequests = requests;
        this.duckDbRateLimited = rateLimitedByStatus;
    }

    Path getPath() {
        return path;
    }

    private void write(Instant ended, CCIndexReaderCounter counter, String status) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("command", command);
        n.put("status", status);
        n.put("tool_git_commit", commit);
        n.put("started", started.toString());
        n.put("ended", ended == null ? null : ended.toString());
        n.put("seed", config.getSeed());
        n.put("seed_generated", config.isSeedGenerated());
        n.put("provenance_path", config.getProvenancePath().toString());
        n.set("config", config.getRawConfig());
        if (counter != null) {
            ObjectNode c = n.putObject("counts");
            c.put("records_read", counter.getRecordsRead().get());
            c.put("files_selected", counter.getFilesExtracted().get());
            c.put("files_written", counter.getFilesWritten().get());
            c.put("truncated", counter.getTruncated().get());
            c.put("empty_payload", counter.getEmptyPayload().get());
            ObjectNode strata = c.putObject("written_by_mime_detected_and_charset");
            for (Map.Entry<String, Long> e : counter.getWrittenByStratum().entrySet()) {
                strata.put(e.getKey(), e.getValue());
            }
        }
        ObjectNode rl = n.putObject("rate_limited");
        rl.put("http_total", rateLimits.getTotal());
        ObjectNode byStatus = rl.putObject("http_by_status");
        for (Map.Entry<Integer, Long> e : rateLimits.getByStatus().entrySet()) {
            byStatus.put(String.valueOf(e.getKey()), e.getValue());
        }
        rl.put("pauses", rateLimits.getTrips());
        if (duckDbRequests >= 0) {
            rl.put("duckdb_requests", duckDbRequests);
            ObjectNode d = rl.putObject("duckdb_by_status");
            for (Map.Entry<Integer, Long> e : duckDbRateLimited.entrySet()) {
                d.put(String.valueOf(e.getKey()), e.getValue());
            }
        }
        try {
            Path parent = path.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            Path tmp = Files.createTempFile(parent, "manifest-", ".tmp");
            Files.write(tmp, MAPPER.writeValueAsBytes(n));
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            LOGGER.warn("couldn't write manifest {}", path, e);
        }
    }

    /** HEAD of the repo holding the running code, else of the cwd; "unknown" if neither. */
    static String gitCommit() {
        File codeDir = null;
        try {
            File loc =
                    new File(
                            RunManifest.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            codeDir = loc.isDirectory() ? loc : loc.getParentFile();
        } catch (URISyntaxException | RuntimeException e) {
            // fall through to cwd
        }
        String c = codeDir == null ? null : revParse(codeDir);
        if (c == null) {
            c = revParse(new File("."));
        }
        return c == null ? "unknown" : c;
    }

    private static String revParse(File dir) {
        try {
            Process p =
                    new ProcessBuilder("git", "rev-parse", "HEAD")
                            .directory(dir)
                            .redirectErrorStream(true)
                            .start();
            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return null;
            }
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            out = out.trim();
            return p.exitValue() == 0 && out.matches("[0-9a-f]{40}") ? out : null;
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
