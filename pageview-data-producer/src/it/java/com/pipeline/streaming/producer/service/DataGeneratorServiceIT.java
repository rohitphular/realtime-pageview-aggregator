package com.pipeline.streaming.producer.service;

import com.pipeline.streaming.producer.model.PageviewEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@Testcontainers
class DataGeneratorServiceIT {

    @Container
    static SaslKafkaContainer kafka = new SaslKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    /* override at producer-scoped path so it takes precedence over the yaml entries
     * that reference KAFKA_PRODUCER_USERNAME / KAFKA_PRODUCER_PASSWORD env vars */
    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.producer.properties.security.protocol", () -> "SASL_PLAINTEXT");
        registry.add("spring.kafka.producer.properties.sasl.mechanism", () -> "PLAIN");
        registry.add("spring.kafka.producer.properties.sasl.jaas.config",
            () -> "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                  "username=\"producer\" password=\"producer-secret\";");
    }

    @Autowired
    private DataGeneratorService dataGeneratorService;

    @Autowired
    private KafkaTemplate<?, ?> kafkaTemplate;

    @Value("${app.kafka.topic:pageviews-raw}")
    private String topic;

    @Test
    void shouldSuccessfullyProducePageviewEvent() {
        dataGeneratorService.generateAndPublishEvent();
        kafkaTemplate.flush();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + SaslKafkaContainer.SASL_USERNAME + "\" " +
            "password=\"" + SaslKafkaContainer.SASL_PASSWORD + "\";");

        try (KafkaConsumer<String, PageviewEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            /* first poll triggers partition assignment and usually returns empty */
            ConsumerRecords<String, PageviewEvent> records = ConsumerRecords.empty();
            long deadline = System.currentTimeMillis() + 15_000;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records = consumer.poll(Duration.ofSeconds(2));
            }

            assertThat(records.isEmpty()).isFalse();
            var record = records.iterator().next();
            assertThat(record.value()).isNotNull();
            assertThat(record.key()).isEqualTo(record.value().getPostcode());
        }
    }
}
