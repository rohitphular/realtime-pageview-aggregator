package com.pipeline.streaming.processor.operator;

import com.pipeline.streaming.avro.PageviewEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class CountAggregatorTest {

    private CountAggregator aggregator;

    @BeforeEach
    void setUp() {
        aggregator = new CountAggregator();
    }

    @Test
    void shouldStartWithZeroAccumulator() {
        assertThat(aggregator.createAccumulator()).isZero();
    }

    @Test
    void shouldIncrementByOnePerEvent() {
        Long acc = aggregator.createAccumulator();

        acc = aggregator.add(new PageviewEvent(1L, "SW19", "/home", 1700000000L), acc);
        assertThat(acc).isEqualTo(1L);

        acc = aggregator.add(new PageviewEvent(2L, "SW19", "/about", 1700000001L), acc);
        assertThat(acc).isEqualTo(2L);
    }

    @Test
    void shouldReturnAccumulatorAsResult() {
        Long acc = 42L;
        assertThat(aggregator.getResult(acc)).isEqualTo(42L);
    }

    @Test
    void shouldMergeAccumulators() {
        assertThat(aggregator.merge(10L, 25L)).isEqualTo(35L);
    }
}
