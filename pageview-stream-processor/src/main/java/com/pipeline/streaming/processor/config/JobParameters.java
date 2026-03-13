package com.pipeline.streaming.processor.config;

import lombok.Getter;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.function.Function;

@Getter
public class JobParameters {

    private final String rawOutputPath;
    private final String aggOutputPath;
    private final String dlqOutputPath;
    private final String bootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final String kafkaUsername;
    private final String kafkaPassword;
    private final String schemaRegistryUrl;
    private final long windowMinutes;

    public JobParameters(ParameterTool parameters) {
        this(parameters, System::getenv);
    }

    /* package-private constructor lets tests inject a null env lookup to isolate from whatever the host has exported */
    JobParameters(ParameterTool parameters, Function<String, String> envLookup) {
        this.rawOutputPath = parameters.get("raw.output.path", "/opt/flink/output/raw");
        this.aggOutputPath = parameters.get("agg.output.path", "/opt/flink/output/aggregated");
        this.dlqOutputPath = parameters.get("dlq.output.path", "/opt/flink/output/dlq");

        this.bootstrapServers = parameters.get("bootstrap.servers", "kafka-broker:29092");
        this.kafkaTopic = parameters.get("kafka.topic", "pageviews-raw");
        this.kafkaGroupId = parameters.get("kafka.group.id", "flink-aggregator-group");
        this.schemaRegistryUrl = parameters.get("schema.registry.url", "http://schema-registry:8081");

        this.kafkaUsername = CredentialHelper.getRequiredEnvOrParam("KAFKA_FLINK_USERNAME", "kafka.sasl.username", parameters, envLookup);
        this.kafkaPassword = CredentialHelper.getRequiredEnvOrParam("KAFKA_FLINK_PASSWORD", "kafka.sasl.password", parameters, envLookup);

        this.windowMinutes = parameters.getLong("window.size.minutes", 1L);
    }
}
