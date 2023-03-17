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

import java.util.Random;

public abstract class AbstractSamplingSelector implements SelectorClause {

    final Sampler sampler;

    AbstractSamplingSelector(Sampler sampler) {
        this.sampler = sampler;
    }
    interface Sampler extends SelectorClause {

    }

    static class SampleAll implements Sampler {
        @Override
        public boolean select(String val) {
            return true;
        }
    }

    static class SampleSome implements Sampler {
        private final double sample;
        private final Random random = new Random();
        SampleSome(double sample) {
            this.sample = sample;
        }

        @Override
        public boolean select(String val) {
            if (random.nextDouble() <= sample) {
                return true;
            }
            return false;
        }
    }
}
