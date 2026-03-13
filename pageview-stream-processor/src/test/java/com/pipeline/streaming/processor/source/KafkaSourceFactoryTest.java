package com.pipeline.streaming.processor.source;

import com.pipeline.streaming.avro.PageviewEvent;
import com.pipeline.streaming.processor.config.JobParameters;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSourceFactoryTest {

    @Test
    void shouldBuildKafkaSourceWithValidParams() {
        /* public constructor falls through to System.getenv — if KAFKA_FLINK_USERNAME is exported on the host it will override the map values here */
        JobParameters params = new JobParameters(ParameterTool.fromMap(Map.of(
                "bootstrap.servers", "localhost:9092",
                "kafka.topic", "test-topic",
                "kafka.group.id", "test-group",
                "kafka.sasl.username", "test-user",
                "kafka.sasl.password", "p@ss=w0rd!#",
                "schema.registry.url", "http://localhost:8085"
        )));

        KafkaSource<PageviewEvent> source = KafkaSourceFactory.build(params);
        assertThat(source).isNotNull();
    }
}
