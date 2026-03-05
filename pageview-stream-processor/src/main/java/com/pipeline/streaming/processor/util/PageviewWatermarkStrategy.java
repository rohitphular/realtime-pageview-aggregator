package com.pipeline.streaming.processor.util;

import com.pipeline.streaming.processor.model.PageviewEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/* applying this post-parse (not at source level) means we lose per-partition watermark tracking;
 * at higher scale a custom KafkaDeserializationSchema would be needed to get source-level watermarks */
public final class PageviewWatermarkStrategy {

    private PageviewWatermarkStrategy() {
    }

    public static WatermarkStrategy<PageviewEvent> build() {
        return WatermarkStrategy
                .<PageviewEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp() * 1000L);
    }
}
