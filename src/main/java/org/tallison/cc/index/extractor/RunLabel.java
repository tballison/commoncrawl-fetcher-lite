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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * Derives a short label from a fetch config (explicit {@code runLabel}, else the {@code docs}
 * output path/bucket) and exposes it as the {@code cc.runLabel} system property so that
 * log4j2.xml can scope each run's CSV logs (extracted-urls.csv, urls-truncated.csv, etc.) to
 * their own subdirectory instead of every config sharing -- and clobbering -- the same files.
 *
 * <p>Must run, and the system property must be set, before any class with a static Logger field
 * (e.g. {@link CCFileExtractor}) is loaded -- that first {@code getLogger()} call is what causes
 * log4j2 to read its configuration and resolve the {@code ${sys:cc.runLabel}} placeholder. This
 * class intentionally has no Logger of its own and parses the config as a plain {@link JsonNode}
 * tree rather than binding to {@link ExtractorConfig}, since binding would construct a default
 * {@code RecordSelector} and trigger its static Logger init prematurely.
 */
public class RunLabel {

    public static final String SYSTEM_PROPERTY = "cc.runLabel";

    private RunLabel() {}

    public static void primeFromConfigFile(String configPath) throws IOException {
        JsonNode root = new ObjectMapper().readTree(new File(configPath));
        System.setProperty(SYSTEM_PROPERTY, resolve(root));
    }

    static String resolve(JsonNode root) {
        String label = textOrNull(root, "runLabel");
        if (isBlank(label)) {
            JsonNode docs = root.path("docs");
            String path = textOrNull(docs, "path");
            if (!isBlank(path)) {
                Path fileName = Paths.get(path).getFileName();
                label = fileName == null ? path : fileName.toString();
            } else {
                String bucket = textOrNull(docs, "bucket");
                if (!isBlank(bucket)) {
                    String prefix = textOrNull(docs, "prefix");
                    label = isBlank(prefix) ? bucket : bucket + "-" + prefix;
                }
            }
        }
        if (isBlank(label)) {
            // matches ExtractorConfig.DEFAULT_FS_DOCS_PATH, the emitter's own fallback
            // when "docs" is omitted entirely
            label = ExtractorConfig.DEFAULT_FS_DOCS_PATH;
        }
        return sanitize(label);
    }

    private static String sanitize(String label) {
        return label.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]", "_");
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }
}
