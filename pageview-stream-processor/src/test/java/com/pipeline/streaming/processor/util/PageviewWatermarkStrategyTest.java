package com.pipeline.streaming.processor.util;

import com.pipeline.streaming.processor.model.PageviewEvent;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PageviewWatermarkStrategyTest {

    private TimestampAssigner<PageviewEvent> assigner;

    @BeforeEach
    void setUp() {
        WatermarkStrategy<PageviewEvent> strategy = PageviewWatermarkStrategy.build();
        assigner = strategy.createTimestampAssigner(null);
    }

    @Test
    void shouldConvertEpochSecondsToMillis() {
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", 1704067200L);

        long timestamp = assigner.extractTimestamp(event, -1L);

        assertThat(timestamp).isEqualTo(1704067200000L);
    }

    @Test
    void shouldConvertRecentEpochSecondsToMillis() {
        long recentEpochSeconds = (System.currentTimeMillis() / 1000L) - 10L;
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", recentEpochSeconds);

        long timestamp = assigner.extractTimestamp(event, -1L);

        assertThat(timestamp).isEqualTo(recentEpochSeconds * 1000L);
    }

    @Test
    void shouldMultiplyByThousandRegardlessOfRecordTimestamp() {
        long epochSeconds = 1_700_000_000L;
        PageviewEvent event = new PageviewEvent(2L, "E14", "/about", epochSeconds);

        long timestamp = assigner.extractTimestamp(event, 9_999L);

        assertThat(timestamp).isEqualTo(epochSeconds * 1_000L);
    }
}
