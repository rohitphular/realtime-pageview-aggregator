package com.pipeline.streaming.processor.operator;

import com.pipeline.streaming.processor.model.PageviewEvent;
import org.apache.flink.api.common.functions.AggregateFunction;


public class CountAggregator implements AggregateFunction<PageviewEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(PageviewEvent event, Long accumulator) {
        return accumulator + 1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}