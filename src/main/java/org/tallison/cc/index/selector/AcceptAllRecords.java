package org.tallison.cc.index.selector;

import org.tallison.cc.index.CCIndexRecord;

public class AcceptAllRecords extends RecordSelector {
    @Override
    public boolean select(CCIndexRecord record) {
        return true;
    }
}
