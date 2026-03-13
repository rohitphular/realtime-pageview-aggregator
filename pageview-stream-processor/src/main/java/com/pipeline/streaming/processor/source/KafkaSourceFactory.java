package com.pipeline.streaming.processor.source;

import com.pipeline.streaming.avro.PageviewEvent;
import com.pipeline.streaming.processor.config.JobParameters;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/* committedOffsets(EARLIEST) is the safe default — resumes from last committed offset and only
 * falls back to EARLIEST on the very first run when no offset has been committed yet */
public final class KafkaSourceFactory {

    private KafkaSourceFactory() {
    }

    public static KafkaSource<PageviewEvent> build(JobParameters params) {
        return KafkaSource.<PageviewEvent>builder()
                .setBootstrapServers(params.getBootstrapServers())
                .setTopics(params.getKafkaTopic())
                .setGroupId(params.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new FaultTolerantAvroDeserializer(params.getSchemaRegistryUrl()))
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                + "username=\"" + params.getKafkaUsername() + "\" "
                                + "password=\"" + params.getKafkaPassword() + "\";")
                .build();
    }
}
