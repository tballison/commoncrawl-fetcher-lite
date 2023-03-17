package org.tallison.cc.index.selector;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({ @JsonSubTypes.Type(MatchSelector.class), @JsonSubTypes.Type(RegexSelector.class)})
public interface SelectorClause {

    boolean select(String val);
}
