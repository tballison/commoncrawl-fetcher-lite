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

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.io.FilenameUtils;

import org.apache.tika.utils.StringUtils;

/**
 * Comma delimited list of extensions to accept. Extensions are currently matched case insensitively.
 */
public class ExtensionsSelector extends AbstractSamplingSelector {
    final Set<String> extensions = new HashSet<>();

    @JsonCreator
    public ExtensionsSelector(@JsonProperty("extensions") String commaDelimitedExtensions,
                              @JsonProperty("sample") Double sample) {
        super(sample == null ? new SampleAll() : new SampleSome(sample));
        for (String ext : commaDelimitedExtensions.split(",")) {
            if (!StringUtils.isBlank(ext)) {
                extensions.add(ext.toLowerCase(Locale.ROOT));
            }
        }
    }

    @Override
    public boolean select(String val) {
        String ext = FilenameUtils.getExtension(val);
        if (! StringUtils.isBlank(ext)) {
            ext = ext.toLowerCase(Locale.ROOT);
            if (extensions.contains(ext)) {
                return sampler.select(val);
            }
        }
        return false;
    }
}
