package org.tallison.cc.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.pipes.emitter.fs.FileSystemEmitter;
import org.apache.tika.pipes.emitter.s3.S3Emitter;
import org.apache.tika.pipes.fetcher.s3.S3Fetcher;
import org.junit.jupiter.api.Test;
import org.tallison.cc.index.fetcher.FetcherConfig;
import org.tallison.cc.index.io.BackoffHttpFetcher;

public class FetcherConfigTest {

    @Test
    public void testBasic() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic-http.json").toURI());
        FetcherConfig fetcherConfig =
                new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        assertEquals(BackoffHttpFetcher.class, fetcherConfig.newFetcher().getClass());
        assertEquals(FileSystemEmitter.class, fetcherConfig.newEmitter().getClass());
    }

    @Test
    public void testLocalIndices() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic-local.json").toURI());
        FetcherConfig fetcherConfig =
                new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        //TODO -- add actual unit test that tests FSFetcher
        assertEquals(BackoffHttpFetcher.class, fetcherConfig.newFetcher().getClass());
        assertEquals(FileSystemEmitter.class, fetcherConfig.newEmitter().getClass());
    }

    @Test
    public void testS3() throws Exception {
        Path p = Paths.get(getClass().getResource("/configs/basic-s3.json").toURI());
        FetcherConfig fetcherConfig =
                new ObjectMapper().readValue(p.toFile(), FetcherConfig.class);

        //TODO -- add actual unit test that tests fetcher and emitter
        assertEquals(S3Fetcher.class, fetcherConfig.newFetcher().getClass());
        assertEquals(S3Emitter.class, fetcherConfig.newEmitter().getClass());
    }
}
