package com.pipeline.streaming.processor.config;

import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/* retain_on_cancellation is important — without it a manual job cancel during an incident wipes
 * the checkpoint state and you lose your offset position on restart */
public final class CheckpointConfigurer {

    private CheckpointConfigurer() {
    }

    public static void configure(StreamExecutionEnvironment env) {
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig config = env.getCheckpointConfig();
        config.setMinPauseBetweenCheckpoints(5_000);
        config.setCheckpointTimeout(60_000);
        config.setMaxConcurrentCheckpoints(1);
        config.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }
}
