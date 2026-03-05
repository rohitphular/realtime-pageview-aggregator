package com.pipeline.streaming.producer.service;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/* testcontainers has no built-in sasl knob — this subclass patches the listener protocol map
 * after tc's own configure() runs, and injects a jaas file via KAFKA_OPTS */
class SaslKafkaContainer extends KafkaContainer {

    static final String SASL_USERNAME = "producer";
    static final String SASL_PASSWORD = "producer-secret";

    private static final String JAAS_FILE_PATH = "/etc/kafka/kafka_server_jaas.conf";

    private static final String JAAS_CONTENT =
        "KafkaServer {\n" +
        "  org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
        "  username=\"admin\"\n" +
        "  password=\"admin-secret\"\n" +
        "  user_admin=\"admin-secret\"\n" +
        "  user_producer=\"producer-secret\";\n" +
        "};\n" +
        "KafkaClient {\n" +
        "  org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
        "  username=\"admin\"\n" +
        "  password=\"admin-secret\";\n" +
        "};\n";

    SaslKafkaContainer(DockerImageName image) {
        super(image);
        withKraft();
        withCopyToContainer(Transferable.of(JAAS_CONTENT), JAAS_FILE_PATH);
        /* cp-kafka entrypoint asserts KAFKA_OPTS is set when sasl is enabled */
        withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=" + JAAS_FILE_PATH);
    }

    @Override
    protected void configure() {
        super.configure();

        /* patch the wire protocol for the external listener from PLAINTEXT to SASL_PLAINTEXT;
         * the listener *name* stays "PLAINTEXT" so tc's startup script still emits valid advertised listeners */
        String currentMap = getEnvMap().getOrDefault(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "");
        String patchedMap = currentMap.replace("PLAINTEXT:PLAINTEXT", "PLAINTEXT:SASL_PLAINTEXT");
        addEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", patchedMap);

        addEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
    }
}
