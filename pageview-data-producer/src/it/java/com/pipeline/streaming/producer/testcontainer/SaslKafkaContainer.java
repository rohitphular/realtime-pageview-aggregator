package com.pipeline.streaming.producer.testcontainer;

import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;


/* enables SASL_PLAINTEXT on the external listener (9092) while leaving BROKER (9093)
 * and CONTROLLER (9094) as PLAINTEXT — schema registry connects via 9093 without credentials */
public class SaslKafkaContainer extends ConfluentKafkaContainer {

    private static final String BROKER_JAAS_CONFIG =
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"admin\" "
                    + "password=\"admin-secret\" "
                    + "user_admin=\"admin-secret\" "
                    + "user_test=\"test-secret\";";

    public static final String CLIENT_JAAS_CONFIG =
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"admin\" "
                    + "password=\"admin-secret\";";

    public SaslKafkaContainer(DockerImageName imageName) {
        super(imageName);
    }

    /* super.configure() must run first — TC 2.0's KafkaHelper.resolveListeners() overwrites the
     * protocol map from scratch, so any env vars set before configure() get clobbered (#10035) */
    @Override
    protected void configure() {
        super.configure();

        /* patch only the external listener from PLAINTEXT to SASL_PLAINTEXT;
         * BROKER and CONTROLLER entries are unaffected because they don't match the replacement */
        String currentMap = getEnvMap().get("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP");
        if (currentMap != null) {
            String patchedMap = currentMap.replace("PLAINTEXT:PLAINTEXT", "PLAINTEXT:SASL_PLAINTEXT");
            addEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", patchedMap);
        }

        /* per-listener JAAS via cp-kafka naming convention: KAFKA_LISTENER_NAME_<LISTENER>_<MECHANISM>_SASL_JAAS_CONFIG */
        addEnv("KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS", "PLAIN");
        addEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", BROKER_JAAS_CONFIG);
        addEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
    }
}
