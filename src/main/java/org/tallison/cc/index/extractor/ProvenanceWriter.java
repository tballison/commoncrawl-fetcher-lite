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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tallison.cc.index.CCIndexRecord;

/** Appends one JSON line per extracted record; thread-safe, file created on first write. */
public class ProvenanceWriter implements Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern CRAWL_ID = Pattern.compile("crawl-data/([^/]+)/");

    private final Path path;
    private BufferedWriter writer;

    public ProvenanceWriter(Path path) {
        this.path = path;
    }

    public synchronized void write(ObjectNode line) throws IOException {
        if (writer == null) {
            if (path.toAbsolutePath().getParent() != null) {
                Files.createDirectories(path.toAbsolutePath().getParent());
            }
            writer =
                    Files.newBufferedWriter(
                            path,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
        }
        writer.write(MAPPER.writeValueAsString(line));
        writer.newLine();
        writer.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    /**
     * @return e.g. CC-MAIN-2025-08, or null if the filename doesn't carry one
     */
    static String crawlId(String warcFilename) {
        if (warcFilename == null) {
            return null;
        }
        Matcher m = CRAWL_ID.matcher(warcFilename);
        return m.find() ? m.group(1) : null;
    }

    static ObjectNode toJson(
            CCIndexRecord r,
            String sha256,
            long storedLength,
            String outputPath,
            Map<String, List<String>> httpHeaders) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("url", r.getUrl());
        n.put("mime", r.getMime());
        n.put("mime_detected", r.getMimeDetected());
        n.put("status", r.getStatus());
        n.put("digest", r.getDigest());
        n.put("length", r.getLength());
        n.put("offset", r.getOffset());
        n.put("filename", r.getFilename());
        n.put("charset", r.getCharset());
        n.put("languages", r.getLanguages());
        n.put("truncated", r.getTruncated());
        n.put("redirect", r.getRedirect());
        n.put("crawl_id", crawlId(r.getFilename()));
        n.put("sha256", sha256);
        n.put("stored_length", storedLength);
        n.put("output_path", outputPath);
        if (httpHeaders == null) {
            n.putNull("http_headers");
        } else {
            ObjectNode h = n.putObject("http_headers");
            for (Map.Entry<String, List<String>> e : httpHeaders.entrySet()) {
                ArrayNode values = h.putArray(e.getKey());
                e.getValue().forEach(values::add);
            }
        }
        return n;
    }
}
