package com.pipeline.streaming.producer.service;

import com.pipeline.streaming.avro.PageviewEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@Testcontainers
class DataGeneratorServiceIT {

    private static final Network SHARED_NETWORK = Network.newNetwork();

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
            .withNetwork(SHARED_NETWORK)
            .withNetworkAliases("kafka-broker");

    @Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.6.0"))
            .withNetwork(SHARED_NETWORK)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka-broker:9093")
            .dependsOn(kafka);

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        /* disable SASL for the test — production auth is orthogonal to Avro/SR validation */
        registry.add("spring.kafka.producer.properties.security.protocol", () -> "PLAINTEXT");
        registry.add("spring.kafka.producer.properties.sasl.mechanism", () -> "");
        registry.add("spring.kafka.producer.properties.sasl.jaas.config", () -> "");
        registry.add("spring.kafka.producer.properties.schema.registry.url",
            () -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));
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

        String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", "true");

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
            assertThat(record.value()).isInstanceOf(PageviewEvent.class);
            assertThat(record.key()).isEqualTo(record.value().getPostcode());
        }
    }
}
