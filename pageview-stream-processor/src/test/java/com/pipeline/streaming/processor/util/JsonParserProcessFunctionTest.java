package com.pipeline.streaming.processor.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.streaming.processor.model.PageviewEvent;
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

class JsonParserProcessFunctionTest {

    private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq-messages", TypeInformation.of(String.class));

    private final ObjectMapper objectMapper = new ObjectMapper();
    private OneInputStreamOperatorTestHarness<String, PageviewEvent> harness;

    @BeforeEach
    void setUp() throws Exception {
        JsonParserProcessFunction function = new JsonParserProcessFunction(DLQ_TAG);
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
    void shouldParseValidJsonToPageviewEvent() throws Exception {
        String validJson = objectMapper.writeValueAsString(new PageviewEvent(1L, "SW19", "/home", 1700000000L));

        harness.processElement(new StreamRecord<>(validJson));

        List<PageviewEvent> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);

        PageviewEvent event = output.get(0);
        assertThat(event.getUserId()).isEqualTo(1L);
        assertThat(event.getPostcode()).isEqualTo("SW19");
        assertThat(event.getWebpage()).isEqualTo("/home");
        assertThat(event.getTimestamp()).isEqualTo(1700000000L);
    }

    @Test
    void shouldRouteMalformedJsonToDlq() throws Exception {
        harness.processElement(new StreamRecord<>("{ not valid json }}}"));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
        assertThat(dlqOutput.peek().getValue()).isEqualTo("{ not valid json }}}");
    }

    @Test
    void shouldRouteNullPostcodeToDlq() throws Exception {
        String json = objectMapper.writeValueAsString(new PageviewEvent(1L, null, "/home", 1700000000L));

        harness.processElement(new StreamRecord<>(json));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldRouteBlankPostcodeToDlq() throws Exception {
        String json = objectMapper.writeValueAsString( new PageviewEvent(1L, "  ", "/home", 1700000000L));

        harness.processElement(new StreamRecord<>(json));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldIgnoreUnknownFieldsInJson() throws Exception {
        String jsonWithExtra = "{\"user_id\":1,\"postcode\":\"SW19\",\"webpage\":\"/home\",\"timestamp\":1700000000,\"extra_field\":\"ignored\"}";

        harness.processElement(new StreamRecord<>(jsonWithExtra));

        List<PageviewEvent> output = harness.extractOutputValues();
        assertThat(output).hasSize(1);
        assertThat(output.get(0).getPostcode()).isEqualTo("SW19");
    }

    @Test
    void shouldRoutePreEpochTimestampToDlq() throws Exception {
        String json = objectMapper.writeValueAsString(new PageviewEvent(1L, "SW19", "/home", 1_500_000_000L));

        harness.processElement(new StreamRecord<>(json));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
        assertThat(dlqOutput.peek().getValue()).isEqualTo(json);
    }

    @Test
    void shouldRouteFarFutureTimestampToDlq() throws Exception {
        long futureEpochSeconds = (System.currentTimeMillis() / 1000L) + 7_200L;
        String json = objectMapper.writeValueAsString(new PageviewEvent(1L, "SW19", "/home", futureEpochSeconds));

        harness.processElement(new StreamRecord<>(json));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldRouteZeroTimestampToDlq() throws Exception {
        String json = objectMapper.writeValueAsString(new PageviewEvent(1L, "SW19", "/home", 0L));

        harness.processElement(new StreamRecord<>(json));

        assertThat(harness.extractOutputValues()).isEmpty();

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }

    @Test
    void shouldAcceptEventWithRecentValidTimestamp() throws Exception {
        long recentEpochSeconds = (System.currentTimeMillis() / 1000L) - 10L;
        String json = objectMapper.writeValueAsString(new PageviewEvent(1L, "SW19", "/home", recentEpochSeconds));

        harness.processElement(new StreamRecord<>(json));

        assertThat(harness.extractOutputValues()).hasSize(1);
        assertThat(harness.getSideOutput(DLQ_TAG)).isNullOrEmpty();
    }

    @Test
    void shouldHandleMultipleEventsInSequence() throws Exception {
        String json1 = objectMapper.writeValueAsString(new PageviewEvent(1L, "SW19", "/home", 1700000000L));
        String json2 = objectMapper.writeValueAsString(new PageviewEvent(2L, "E14", "/about", 1700000001L));
        String badJson = "not json";

        harness.processElement(new StreamRecord<>(json1));
        harness.processElement(new StreamRecord<>(badJson));
        harness.processElement(new StreamRecord<>(json2));

        assertThat(harness.extractOutputValues()).hasSize(2);

        Queue<StreamRecord<String>> dlqOutput = harness.getSideOutput(DLQ_TAG);
        assertThat(dlqOutput).hasSize(1);
    }
}
