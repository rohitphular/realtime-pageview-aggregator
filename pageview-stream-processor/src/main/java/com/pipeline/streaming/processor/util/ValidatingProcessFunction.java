package com.pipeline.streaming.processor.util;

import com.pipeline.streaming.avro.PageviewEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates semantics of already-deserialized PageviewEvent records.
 * Routes invalid records (null/blank postcode, out-of-bounds timestamp) to
 * a DLQ side output. JSON parsing is no longer needed — Avro deserialization
 * happens at the source level.
 */
public class ValidatingProcessFunction extends ProcessFunction<PageviewEvent, PageviewEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidatingProcessFunction.class);

    /* bounds live here so the watermark strategy doesn't need to duplicate them —
     * any event with a timestamp outside [2020-01-01, now+1h] goes to the dlq */
    static final long MIN_VALID_TIMESTAMP_MS = 1_577_836_800_000L;
    static final long MAX_FUTURE_DRIFT_MS    = 3_600_000L;

    private final OutputTag<String> dlqTag;

    /* flink's MetricGroup throws if you register the same counter name twice in the same operator */
    private transient Counter dlqCounter;

    public ValidatingProcessFunction(OutputTag<String> dlqTag) {
        this.dlqTag = dlqTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dlqCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("dlq_routed_count");
    }

    @Override
    public void processElement(PageviewEvent event, Context context, Collector<PageviewEvent> out) {

        String postcode = event.getPostcode();
        if (postcode == null || postcode.isBlank()) {
            LOG.warn("Event with null/blank postcode routed to DLQ. userId={}", event.getUserId());
            dlqCounter.inc();
            context.output(dlqTag, event.toString());
            return;
        }

        long tsMillis = event.getTimestamp() * 1000L;
        long now = System.currentTimeMillis();
        if (tsMillis < MIN_VALID_TIMESTAMP_MS || tsMillis > now + MAX_FUTURE_DRIFT_MS) {
            LOG.warn("Event with out-of-bounds timestamp ({} epoch-s) routed to DLQ. userId={}",
                    event.getTimestamp(), event.getUserId());
            dlqCounter.inc();
            context.output(dlqTag, event.toString());
            return;
        }

        out.collect(event);
    }
}
