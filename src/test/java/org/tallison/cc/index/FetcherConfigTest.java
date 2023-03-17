package org.tallison.cc.index;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.tallison.cc.index.fetcher.FetcherConfig;

public class FetcherConfigTest {

    @Test
    public void testBasic() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic.json").toURI());
        FetcherConfig fetcherConfig =
                new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        System.out.println(new CCIndexReaderCounter());
    }
}
