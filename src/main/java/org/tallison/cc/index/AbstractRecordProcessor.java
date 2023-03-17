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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public abstract class AbstractRecordProcessor implements IndexRecordProcessor {

    protected static AtomicInteger threadCounter = new AtomicInteger(0);

    private final int threadNumber;

    public AbstractRecordProcessor() {
        threadNumber = threadCounter.incrementAndGet();
    }


    protected int getThreadNumber() {
        return threadNumber;
    }

    String getExtension(String u) {
        if (u == null || u.length() == 0) {
            return null;
        }
        int i = u.lastIndexOf('.');
        if (i < 0 || i+6 < u.length()) {
            return null;
        }
        String ext = u.substring(i+1);
        ext = ext.trim();
        Matcher m = Pattern.compile("^\\d+$").matcher(ext);
        if (m.find()) {
            return null;
        }
        ext = ext.toLowerCase(Locale.ENGLISH);
        ext = ext.replaceAll("\\/$", "");
        return ext;
    }

    //returns "" if key is null, otherwise, trims and converts remaining \r\n\t to " "
    protected static String clean(String key) {
        if (key == null) {
            return "";
        }
        return key.trim().replaceAll("[\r\n\t]", " ");
    }

}
