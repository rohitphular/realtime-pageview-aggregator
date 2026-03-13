package com.pipeline.streaming.processor.util;

import com.pipeline.streaming.avro.PageviewEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

class ValidatingProcessFunctionTest {

    private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq-messages", TypeInformation.of(String.class));

    private OneInputStreamOperatorTestHarness<PageviewEvent, PageviewEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        ValidatingProcessFunction function = new ValidatingProcessFunction(DLQ_TAG);
        harness = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(function));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void shouldPassValidEvent() throws Exception {
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", 1700000000L);

        harness.processElement(new StreamRecord<>(event));

        List<PageviewEvent> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);

        PageviewEvent result = output.get(0);
        assertThat(result.getUserId()).isEqualTo(1L);
        assertThat(result.getPostcode()).isEqualTo("SW19");
        assertThat(result.getWebpage()).isEqualTo("/home");
        assertThat(result.getTimestamp()).isEqualTo(1700000000L);
    }

    @Test
    void shouldRouteNullPostcodeToDlq() throws Exception {
        PageviewEvent event = new PageviewEvent(1L, null, "/home", 1700000000L);

        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldRouteBlankPostcodeToDlq() throws Exception {
        PageviewEvent event = new PageviewEvent(1L, "  ", "/home", 1700000000L);

        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldRoutePreEpochTimestampToDlq() throws Exception {
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", 1_500_000_000L);

        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldRouteFarFutureTimestampToDlq() throws Exception {
        long futureEpochSeconds = (System.currentTimeMillis() / 1000L) + 7_200L;
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", futureEpochSeconds);

        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldRouteZeroTimestampToDlq() throws Exception {
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", 0L);

        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldAcceptEventWithRecentValidTimestamp() throws Exception {
        long recentEpochSeconds = (System.currentTimeMillis() / 1000L) - 10L;
        PageviewEvent event = new PageviewEvent(1L, "SW19", "/home", recentEpochSeconds);

        harness.processElement(new StreamRecord<>(event));

        assertThat(harness.extractOutputValues()).hasSize(1);
        assertThat(harness.getSideOutput(DLQ_TAG)).isNullOrEmpty();
    }

    @Test
    void shouldHandleMultipleEventsInSequence() throws Exception {
        PageviewEvent valid1 = new PageviewEvent(1L, "SW19", "/home", 1700000000L);
        PageviewEvent invalid = new PageviewEvent(2L, null, "/about", 1700000001L);
        PageviewEvent valid2 = new PageviewEvent(3L, "E14", "/contact", 1700000002L);

        harness.processElement(new StreamRecord<>(valid1));
        harness.processElement(new StreamRecord<>(invalid));
        harness.processElement(new StreamRecord<>(valid2));

        assertThat(harness.extractOutputValues()).hasSize(2);

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }
}
