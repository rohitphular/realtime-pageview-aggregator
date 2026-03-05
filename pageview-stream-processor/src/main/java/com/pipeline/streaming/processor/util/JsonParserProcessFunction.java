package com.pipeline.streaming.processor.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pipeline.streaming.processor.model.PageviewEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


@Slf4j
public class JsonParserProcessFunction extends ProcessFunction<String, PageviewEvent> {

    /* bounds live here so the watermark strategy doesn't need to duplicate them —
     * any event with a timestamp outside [2020-01-01, now+1h] goes to the dlq */
    static final long MIN_VALID_TIMESTAMP_MS = 1_577_836_800_000L;
    static final long MAX_FUTURE_DRIFT_MS    = 3_600_000L;

    private transient ObjectMapper objectMapper;
    private final OutputTag<String> dlqTag;

    /* flink's MetricGroup throws if you register the same counter name twice in the same operator */
    private transient Counter dlqCounter;

    public JsonParserProcessFunction(OutputTag<String> dlqTag) {
        this.dlqTag = dlqTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        /* jackson needs JavaTimeModule registered or Instant fields silently deserialize as epoch-millis strings */
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());

        this.dlqCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("dlq_routed_count");
    }

    @Override
    public void processElement(String json, Context context, Collector<PageviewEvent> out) {
        try {
            PageviewEvent event = objectMapper.readValue(json, PageviewEvent.class);

            /* keyBy(getPostcode()) throws a nullpointerexception on null keys — discovered this the hard way */
            if (event.getPostcode() == null || event.getPostcode().isBlank()) {
                log.warn("Event with null/blank postcode routed to DLQ. Payload (truncated): {}", truncate(json));
                dlqCounter.inc();
                context.output(dlqTag, json);
                return;
            }

            long tsMillis = event.getTimestamp() * 1000L;
            long now = System.currentTimeMillis();
            if (tsMillis < MIN_VALID_TIMESTAMP_MS || tsMillis > now + MAX_FUTURE_DRIFT_MS) {
                log.warn("Event with out-of-bounds timestamp ({} epoch-s) routed to DLQ. Payload (truncated): {}",
                        event.getTimestamp(), truncate(json));
                dlqCounter.inc();
                context.output(dlqTag, json);
                return;
            }

            out.collect(event);
        } catch (Exception e) {
            log.warn("Failed to parse JSON, routing to DLQ. Reason: {}. Payload (truncated): {}", e.getMessage(), truncate(json));
            dlqCounter.inc();
            context.output(dlqTag, json);
        }
    }

    private static String truncate(String input) {
        if (input == null) return "null";
        return input.length() <= 200 ? input : input.substring(0, 200) + "...";
    }
}
