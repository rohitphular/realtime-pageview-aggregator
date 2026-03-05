package com.pipeline.streaming.processor.operator;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;


public class WindowResultFormatter extends ProcessWindowFunction<Long, String, String, TimeWindow> {

    @Override
    public void process(String postcode, Context context, Iterable<Long> elements, Collector<String> out) {
        Long count = elements.iterator().next();

        String windowStart = Instant.ofEpochMilli(context.window().getStart()).toString();
        String windowEnd = Instant.ofEpochMilli(context.window().getEnd()).toString();

        out.collect(String.format(
                "{\"postcode\":\"%s\",\"window_start\":\"%s\",\"window_end\":\"%s\",\"count\":%d}",
                postcode, windowStart, windowEnd, count));
    }
}
