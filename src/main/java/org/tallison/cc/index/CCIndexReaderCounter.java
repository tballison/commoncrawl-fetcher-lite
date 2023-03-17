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

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

public class CCIndexReaderCounter {
    DecimalFormat df = new DecimalFormat("###,###");
    AtomicLong recordsRead = new AtomicLong(0);
    AtomicLong filesExtracted = new AtomicLong(0);
    AtomicLong truncatedWritten = new AtomicLong(0);

    public AtomicLong getRecordsRead() {
        return recordsRead;
    }

    public AtomicLong getFilesExtracted() {
        return filesExtracted;
    }

    public AtomicLong getTruncatedWritten() {
        return truncatedWritten;
    }

    @Override
    public String toString() {
        return "counts: {" +
                "recordsRead=" + String.format("%,d", recordsRead.get()) +
                ", filesExtracted=" + String.format("%,d", filesExtracted.get()) +
                ", truncatedWritten=" + String.format("%,d", truncatedWritten.get()) +
                '}';
    }
}