package com.pipeline.streaming.processor.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class JobParametersTest {

    /* null env lookup isolates tests from whatever KAFKA_FLINK_USERNAME/PASSWORD the makefile may have exported */
    private static final Function<String, String> NO_ENV = key -> null;

    @Test
    void shouldUseDefaultValuesWhenNoParametersProvided() {
        ParameterTool params = ParameterTool.fromMap(Map.of("kafka.sasl.username", "user", "kafka.sasl.password", "pass"));

        JobParameters jobParams = new JobParameters(params, NO_ENV);

        assertThat(jobParams.getRawOutputPath()).isEqualTo("/opt/flink/output/raw");
        assertThat(jobParams.getAggOutputPath()).isEqualTo("/opt/flink/output/aggregated");
        assertThat(jobParams.getDlqOutputPath()).isEqualTo("/opt/flink/output/dlq");
        assertThat(jobParams.getBootstrapServers()).isEqualTo("kafka-broker:29092");
        assertThat(jobParams.getKafkaTopic()).isEqualTo("pageviews-raw");
        assertThat(jobParams.getKafkaGroupId()).isEqualTo("flink-aggregator-group");
        assertThat(jobParams.getWindowMinutes()).isEqualTo(1L);
    }

    @Test
    void shouldUseCustomValuesWhenProvided() {
        ParameterTool params = ParameterTool.fromMap(Map.of(
                "raw.output.path", "/custom/raw",
                "agg.output.path", "/custom/agg",
                "dlq.output.path", "/custom/dlq",
                "bootstrap.servers", "my-broker:9092",
                "kafka.topic", "custom-topic",
                "kafka.group.id", "custom-group",
                "kafka.sasl.username", "custom-user",
                "kafka.sasl.password", "custom-pass",
                "window.size.minutes", "5"
        ));

        JobParameters jobParams = new JobParameters(params, NO_ENV);

        assertThat(jobParams.getRawOutputPath()).isEqualTo("/custom/raw");
        assertThat(jobParams.getAggOutputPath()).isEqualTo("/custom/agg");
        assertThat(jobParams.getDlqOutputPath()).isEqualTo("/custom/dlq");
        assertThat(jobParams.getBootstrapServers()).isEqualTo("my-broker:9092");
        assertThat(jobParams.getKafkaTopic()).isEqualTo("custom-topic");
        assertThat(jobParams.getKafkaGroupId()).isEqualTo("custom-group");
        assertThat(jobParams.getKafkaUsername()).isEqualTo("custom-user");
        assertThat(jobParams.getKafkaPassword()).isEqualTo("custom-pass");
        assertThat(jobParams.getWindowMinutes()).isEqualTo(5L);
    }
}
