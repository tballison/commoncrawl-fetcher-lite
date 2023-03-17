package org.tallison.cc.index.selector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RegexSelector extends AbstractSamplingSelector {

    final Pattern pattern;

    @JsonCreator
    public RegexSelector(@JsonProperty("pattern") String pattern,
                         @JsonProperty("sample") Double sample) {
        super(sample == null ? new SampleAll() : new SampleSome(sample));
        this.pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean select(String val) {
        Matcher m = pattern.matcher(val);
        if (m.find()) {
            return sampler.select(val);
        }
        return false;
    }

}
