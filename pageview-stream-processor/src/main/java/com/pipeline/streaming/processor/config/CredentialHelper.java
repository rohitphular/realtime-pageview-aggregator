package com.pipeline.streaming.processor.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/* sasl credentials must come from env vars — flink logs the full parameter map on startup via setGlobalJobParameters,
 * so anything passed as a cli arg will appear in plaintext in the jobmanager logs */
public final class CredentialHelper {

    private static final Set<String> SECRET_KEYS = Set.of("kafka.sasl.username", "kafka.sasl.password");

    private CredentialHelper() {
    }

    public static String getRequiredEnvOrParam(String envKey, String paramKey, ParameterTool parameters) {
        return getRequiredEnvOrParam(envKey, paramKey, parameters, System::getenv);
    }

    static String getRequiredEnvOrParam(String envKey, String paramKey, ParameterTool parameters,
                                        Function<String, String> envLookup) {
        String value = envLookup.apply(envKey);
        if (value != null && !value.isBlank()) {
            return value;
        }

        return parameters.getRequired(paramKey);
    }

    public static ParameterTool scrubSecrets(ParameterTool parameters) {
        Map<String, String> cleaned = new HashMap<>(parameters.toMap());
        SECRET_KEYS.forEach(cleaned::remove);
        return ParameterTool.fromMap(cleaned);
    }
}
