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
