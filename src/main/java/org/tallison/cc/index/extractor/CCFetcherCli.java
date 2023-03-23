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

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * This is a lighter class that doesn't rely on a database
 * to extract files from CC and write a list of truncated urls.
 */
public class CCFetcherCli {

    public static void main(String[] args) throws Exception {
        String command = args[0];

        if (command.equals("Fetch")) {
            CCFileExtractor.main(new String[]{args[1]});
        } else if (command.equals("FetchIndices")) {
            CCIndexFetcher.main(new String[]{args[1]});
        } else if (command.equals("CountMimes")) {
            CCMimeCounter.main(new String[]{args[1]});
        } else if (Files.isRegularFile(Paths.get(command))) {
            CCFileExtractor.main(new String[]{args[0]});
        } else {
            System.out.println("Must start with a command: Fetch, FetchIndices or CountMimes");
        }
    }
}
