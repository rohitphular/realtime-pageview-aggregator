package com.pipeline.streaming.processor.source;

import com.pipeline.streaming.avro.PageviewEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps ConfluentRegistryAvroDeserializationSchema to catch deserialization
 * failures and prevent poison-pill restart loops. Failed records are logged
 * and counted; the downstream ValidatingProcessFunction handles DLQ routing
 * for structurally valid but semantically invalid records.
 */
public class FaultTolerantAvroDeserializer implements DeserializationSchema<PageviewEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FaultTolerantAvroDeserializer.class);

    private final ConfluentRegistryAvroDeserializationSchema<PageviewEvent> inner;
    private transient long deserializationFailures = 0;

    public FaultTolerantAvroDeserializer(String schemaRegistryUrl) {
        this.inner = ConfluentRegistryAvroDeserializationSchema.forSpecific(PageviewEvent.class, schemaRegistryUrl);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        inner.open(context);
    }

    @Override
    public PageviewEvent deserialize(byte[] message) throws IOException {
        try {
            return inner.deserialize(message);
        } catch (Exception e) {
            deserializationFailures++;
            LOG.warn("Avro deserialization failed (total failures: {}). Dropping record ({} bytes). Reason: {}", deserializationFailures, message != null ? message.length : 0, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(PageviewEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PageviewEvent> getProducedType() {
        return inner.getProducedType();
    }
}
