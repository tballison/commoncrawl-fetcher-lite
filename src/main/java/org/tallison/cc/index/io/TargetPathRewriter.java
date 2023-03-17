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
package org.tallison.cc.index.io;

import java.util.ArrayList;
import java.util.List;

public class TargetPathRewriter {

    List<Integer> offsets = new ArrayList<>();

    public TargetPathRewriter(String targetPathPattern) {
        if (targetPathPattern.startsWith("/")) {
            throw new IllegalArgumentException("targetPathRewriter cannot start with '/'");
        }
        if (targetPathPattern.endsWith("/")) {
            throw new IllegalArgumentException("targetPathRewriter cannot end with '/'");
        }

        int i = targetPathPattern.indexOf('/');
        int hits = 0;
        while (i > -1) {
            offsets.add(i - hits);
            hits++;
            i = targetPathPattern.indexOf('/', i + 1);
        }
    }

    public String rewrite(String originalPath) {
        if (offsets.size() == 0) {
            return originalPath;
        }
        StringBuilder sb = new StringBuilder();
        int start = 0;
        for (int i : offsets) {
            sb.append(originalPath.substring(start, i));
            sb.append('/');
            start = i;
        }
        sb.append(originalPath);
        return sb.toString();
    }
}
