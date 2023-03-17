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
