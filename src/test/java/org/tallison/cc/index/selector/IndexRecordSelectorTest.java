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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.tallison.cc.index.CCIndexRecord;

public class IndexRecordSelectorTest {

    @Test
    public void testBasic() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        //this just tests that the deserialization works
        RecordSelector recordSelector =
                mapper.readValue(getClass().getResourceAsStream("/selectors/basic.json"),
                        RecordSelector.class);
    }

    @Test
    @Disabled("for development only")
    public void testIndexFile() throws Exception {
        Path p = Paths.get("/...CC-MAIN-2023-06/indexes/cdx-00000.gz");
        ObjectMapper mapper = new ObjectMapper();
        //this just tests that the deserialization works
        RecordSelector recordSelector =
                mapper.readValue(getClass().getResourceAsStream("/selectors/extensions.json"),
                        RecordSelector.class);
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(Files.newInputStream(p)),
                        StandardCharsets.UTF_8))) {
            String line = r.readLine();
            while (line != null) {
                Optional<CCIndexRecord> record = CCIndexRecord.parseRecord(line);
                if (record.isPresent()) {
                    CCIndexRecord indexRecord = record.get();
                    if (recordSelector.select(indexRecord)) {
                        System.out.println("Selected: " + indexRecord);
                    } else {
                        //System.out.println("Rejected: " + indexRecord.getUrl());
                    }
                }
                line = r.readLine();
            }
        }
    }
}
