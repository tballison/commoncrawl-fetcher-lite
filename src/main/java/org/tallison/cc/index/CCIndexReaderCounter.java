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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

public class CCIndexReaderCounter {
    AtomicLong recordsRead = new AtomicLong(0);
    AtomicLong filesExtracted = new AtomicLong(0);
    AtomicLong truncated = new AtomicLong(0);
    AtomicLong emptyPayload = new AtomicLong(0);
    AtomicLong indexFilesCompleted = new AtomicLong(0);
    private int totalIndexFiles = -1;

    public AtomicLong getRecordsRead() {
        return recordsRead;
    }

    public AtomicLong getFilesExtracted() {
        return filesExtracted;
    }

    public AtomicLong getTruncated() {
        return truncated;
    }

    public AtomicLong getEmptyPayload() {
        return emptyPayload;
    }

    public AtomicLong getIndexFilesCompleted() {
        return indexFilesCompleted;
    }

    public int getTotalIndexFiles() {
        return totalIndexFiles;
    }

    public void setTotalIndexFiles(int totalIndexFiles) {
        this.totalIndexFiles = totalIndexFiles;
    }

    /**
     * Returns a formatted progress summary suitable for logging.
     */
    public String progressSummary() {
        long completed = indexFilesCompleted.get();
        long records = recordsRead.get();
        long extracted = filesExtracted.get();
        long trunc = truncated.get();

        StringBuilder sb = new StringBuilder();
        if (totalIndexFiles > 0) {
            double pct = 100.0 * completed / totalIndexFiles;
            sb.append(String.format(Locale.US,
                    "index files: %,d/%,d (%.1f%%)",
                    completed, totalIndexFiles, pct));
        } else {
            sb.append(String.format(Locale.US,
                    "index files: %,d completed", completed));
        }
        sb.append(String.format(Locale.US,
                " | records: %,d | extracted: %,d | truncated: %,d",
                records, extracted, trunc));
        return sb.toString();
    }

    @Override
    public String toString() {
        return "CCIndexReaderCounter{"
                + "recordsRead="
                + recordsRead
                + ", filesExtracted="
                + filesExtracted
                + ", truncated="
                + truncated
                + ", emptyPayload="
                + emptyPayload
                + '}';
    }
}
