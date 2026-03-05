package com.pipeline.streaming.processor.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public final class FileSinkFactory {

    private static final DefaultRollingPolicy<String, String> ROLLING_POLICY = DefaultRollingPolicy.builder()
            .withRolloverInterval(Duration.ofMinutes(1))
            .withInactivityInterval(Duration.ofSeconds(30))
            .withMaxPartSize(MemorySize.ofMebiBytes(10))
            .build();

    private FileSinkFactory() {
    }

    public static FileSink<String> build(String outputPath) {
        return FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(ROLLING_POLICY)
                .build();
    }
}
