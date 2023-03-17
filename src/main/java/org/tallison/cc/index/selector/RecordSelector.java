package org.tallison.cc.index.selector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.tallison.cc.index.CCIndexRecord;

public class RecordSelector {

    private static class AcceptAllRecords extends RecordSelector {
        @Override
        public boolean select(CCIndexRecord record) {
            return true;
        }
    }

    public static RecordSelector ACCEPT_ALL_RECORDS = new AcceptAllRecords();

    @JsonProperty
    Map<String, List<SelectorClause>> must = new HashMap<>();
    @JsonProperty
    Map<String, List<SelectorClause>> must_not = new HashMap<>();
    @JsonProperty
    Map<String, List<SelectorClause>> should = new HashMap<>();

    public boolean select(CCIndexRecord record) {

        for (Map.Entry<String, List<SelectorClause>> e : must_not.entrySet()) {
            String val = getStringValue(e.getKey(), record);
            for (SelectorClause clause : e.getValue()) {
                if (clause.select(val)) {
                    return false;
                }
            }
        }

        for (Map.Entry<String, List<SelectorClause>> e : must.entrySet()) {
            String val = getStringValue(e.getKey(), record);
            for (SelectorClause clause : e.getValue()) {
                if (!clause.select(val)) {
                    return false;
                }
            }
        }

        for (Map.Entry<String, List<SelectorClause>> e : should.entrySet()) {
            String val = getStringValue(e.getKey(), record);
            for (SelectorClause clause : e.getValue()) {
                if (clause.select(val)) {
                    return true;
                }
            }
        }
        return false;
    }

    private String getStringValue(String key, CCIndexRecord record) {

        switch (key) {
            case "mime_detected" :
                return record.getMimeDetected();
            case "truncated" :
                return record.getTruncated();
            case "mime" :
                return record.getMime();
            case "status" :
                return Integer.toString(record.getStatus());
            case "url":
                return record.getUrl();
            case "host":
                return record.getHost();
            case "digest":
                return record.getDigest();
            default:
                throw new IllegalArgumentException("Don't yet support key " + key);
        }
    }

}
