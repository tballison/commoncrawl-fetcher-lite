package org.tallison.cc.index.selector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * match exact string
 */
public class MatchSelector extends AbstractSamplingSelector {

    private static final boolean DEFAULT_CASE_SENSITIVE = true;
    final boolean caseSensitive;
    final String match;

    @JsonCreator
    MatchSelector(@JsonProperty("match") String match,
                  @JsonProperty("sample") Double sample,
                  @JsonProperty("case_sensitive") Boolean caseSensitive) {
        super(sample == null ? new SampleAll() : new SampleSome(sample));
        this.match = match;
        this.caseSensitive = caseSensitive == null ? DEFAULT_CASE_SENSITIVE : caseSensitive;
    }

    @Override
    public boolean select(String val) {
        if (caseSensitive) {
            if (match.equals(val)) {
                return true;
            }
        } else {
            if (match.equalsIgnoreCase(val)) {
                return true;
            }
        }
        return false;
    }
}
