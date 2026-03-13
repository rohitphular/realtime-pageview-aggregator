package com.pipeline.streaming.processor.util;

import com.pipeline.streaming.avro.PageviewEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/* with avro deserialization at the source, this strategy can now be applied at source level
 * enabling per-partition watermark tracking — a real operational improvement at scale */
public final class PageviewWatermarkStrategy {

    private PageviewWatermarkStrategy() {
    }

    public static WatermarkStrategy<PageviewEvent> build() {
        return WatermarkStrategy
                .<PageviewEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp() * 1000L);
    }
}
