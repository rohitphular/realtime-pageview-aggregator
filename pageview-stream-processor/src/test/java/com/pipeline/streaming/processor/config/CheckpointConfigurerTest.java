package com.pipeline.streaming.processor.config;

import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointConfigurerTest {

    @Test
    void shouldConfigureExactlyOnceCheckpointing() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurer.configure(env);

        CheckpointConfig config = env.getCheckpointConfig();
        assertThat(config.getCheckpointingConsistencyMode()).isEqualTo(CheckpointingMode.EXACTLY_ONCE);
    }

    @Test
    void shouldSetCheckpointInterval() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurer.configure(env);

        assertThat(env.getCheckpointInterval()).isEqualTo(10_000L);
    }

    @Test
    void shouldSetMinPauseBetweenCheckpoints() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurer.configure(env);

        assertThat(env.getCheckpointConfig().getMinPauseBetweenCheckpoints()).isEqualTo(5_000L);
    }

    @Test
    void shouldSetCheckpointTimeout() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurer.configure(env);

        assertThat(env.getCheckpointConfig().getCheckpointTimeout()).isEqualTo(60_000L);
    }

    @Test
    void shouldLimitToOneConcurrentCheckpoint() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurer.configure(env);

        assertThat(env.getCheckpointConfig().getMaxConcurrentCheckpoints()).isEqualTo(1);
    }

    @Test
    void shouldRetainCheckpointsOnCancellation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CheckpointConfigurer.configure(env);

        assertThat(env.getCheckpointConfig().getExternalizedCheckpointRetention())
                .isEqualTo(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }
}
