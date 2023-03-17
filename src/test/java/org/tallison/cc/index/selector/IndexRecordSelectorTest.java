package org.tallison.cc.index.selector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.utils.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.tallison.cc.index.CCIndexRecord;


public class IndexRecordSelectorTest {

    @Test
    public void testBasic() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        RecordSelector recordSelector = mapper.readValue(getClass().getResourceAsStream("/selectors/basic.json"),
                RecordSelector.class);
    }

    @Test
    @Disabled("for development only")
    public void testIndexFile() throws Exception {
        Path p = Paths.get("/Users/allison/data/cc/index-work/cdx-00000.gz");
        try (BufferedReader r =
                new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(p))))) {
            String line = r.readLine();
            while (line != null) {
                Optional<CCIndexRecord> record = CCIndexRecord.parseRecord(line);
                if (record.isPresent()) {
                    CCIndexRecord indexRecord = record.get();
                    if (!indexRecord.getMime().equals(indexRecord.getMimeDetected())) {
                        System.out.println(line);
                    }
                    if (!StringUtils.isBlank(indexRecord.getTruncated())) {

                    }
                }
                line = r.readLine();
            }
        }
    }
}
