package org.tallison.cc.index.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TargetPathRewriter {

    List<Integer> offsets = new ArrayList<>();

    public TargetPathRewriter(String targetPathPattern) {
        if (targetPathPattern.startsWith("/")) {
            throw new IllegalArgumentException("targetPathRewriter cannot start with '/'");
        }
        if (targetPathPattern.endsWith("/")) {
            throw new IllegalArgumentException("targetPathRewriter cannot end with '/'");
        }

        int i = targetPathPattern.indexOf('/');
        int hits = 0;
        while (i > -1) {
            offsets.add(i - hits);
            hits++;
            i = targetPathPattern.indexOf('/', i + 1);
        }
    }

    public String rewrite(String originalPath) {
        if (offsets.size() == 0) {
            return originalPath;
        }
        StringBuilder sb = new StringBuilder();
        int start = 0;
        for (int i : offsets) {
            sb.append(originalPath.substring(start, i));
            sb.append('/');
            start = i;
        }
        sb.append(originalPath);
        return sb.toString();
    }
}
