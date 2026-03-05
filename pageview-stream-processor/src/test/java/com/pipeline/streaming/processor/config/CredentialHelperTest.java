package com.pipeline.streaming.processor.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CredentialHelperTest {

    @Test
    void shouldFallBackToParameterWhenEnvNotSet() {
        ParameterTool params = ParameterTool.fromMap(Map.of(
                "kafka.sasl.username", "test-user"
        ));

        String result = CredentialHelper.getRequiredEnvOrParam(
                "UNLIKELY_ENV_VAR_FOR_TEST_XYZ_123", "kafka.sasl.username", params);

        assertThat(result).isEqualTo("test-user");
    }

    @Test
    void shouldThrowWhenNeitherEnvNorParamIsSet() {
        ParameterTool params = ParameterTool.fromMap(Map.of());

        assertThatThrownBy(() -> CredentialHelper.getRequiredEnvOrParam(
                "UNLIKELY_ENV_VAR_FOR_TEST_XYZ_123", "missing.key", params))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldScrubSecretKeysFromParameters() {
        ParameterTool params = ParameterTool.fromMap(Map.of(
                "bootstrap.servers", "localhost:9092",
                "kafka.topic", "test-topic",
                "kafka.sasl.username", "secret-user",
                "kafka.sasl.password", "secret-pass"
        ));

        ParameterTool scrubbed = CredentialHelper.scrubSecrets(params);

        assertThat(scrubbed.toMap()).containsKey("bootstrap.servers");
        assertThat(scrubbed.toMap()).containsKey("kafka.topic");
        assertThat(scrubbed.toMap()).doesNotContainKey("kafka.sasl.username");
        assertThat(scrubbed.toMap()).doesNotContainKey("kafka.sasl.password");
    }

    @Test
    void shouldPreserveAllNonSecretKeys() {
        ParameterTool params = ParameterTool.fromMap(Map.of(
                "bootstrap.servers", "localhost:9092",
                "kafka.topic", "my-topic",
                "kafka.group.id", "my-group",
                "window.size.minutes", "5"
        ));

        ParameterTool scrubbed = CredentialHelper.scrubSecrets(params);

        assertThat(scrubbed.toMap()).hasSize(4);
        assertThat(scrubbed.get("bootstrap.servers")).isEqualTo("localhost:9092");
        assertThat(scrubbed.get("kafka.topic")).isEqualTo("my-topic");
        assertThat(scrubbed.get("kafka.group.id")).isEqualTo("my-group");
        assertThat(scrubbed.get("window.size.minutes")).isEqualTo("5");
    }
}
