package com.pipeline.streaming.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.streaming.processor.model.PageviewEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PageviewAggregatorJobIntegrationTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(4)
                    .setNumberTaskManagers(1)
                    .build()
    );

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        RawCollectingSink.VALUES.clear();
        AggCollectingSink.VALUES.clear();
        DlqCollectingSink.VALUES.clear();
    }

    @Test
    void validEvents_shouldProduceAggregatedOutput() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /* bounded source emits Long.MAX_VALUE watermark on completion, which is what fires all pending windows */
        List<String> events = Arrays.asList(
                toJson(new PageviewEvent(1L, "SW19", "/home", 1700000000L)),
                toJson(new PageviewEvent(2L, "SW19", "/about", 1700000010L)),
                toJson(new PageviewEvent(3L, "E14", "/contact", 1700000020L))
        );

        DataStream<String> source = env.fromCollection(events);
        OutputTag<String> dlqTag = new OutputTag<>("dlq-messages", TypeInformation.of(String.class));

        PageviewAggregatorJob.wireTopology(source, 1, dlqTag,
                new RawCollectingSink(), new AggCollectingSink(), new DlqCollectingSink());

        env.execute("Integration Test - Valid Events");

        assertThat(RawCollectingSink.VALUES).hasSize(3);
        assertThat(DlqCollectingSink.VALUES).isEmpty();
        assertThat(AggCollectingSink.VALUES).hasSize(2);

        List<JsonNode> aggResults = new ArrayList<>();
        for (String json : AggCollectingSink.VALUES) {
            aggResults.add(OBJECT_MAPPER.readTree(json));
        }

        JsonNode sw19 = aggResults.stream()
                .filter(n -> "SW19".equals(n.get("postcode").asText()))
                .findFirst().orElseThrow();
        assertThat(sw19.get("count").asLong()).isEqualTo(2L);

        JsonNode e14 = aggResults.stream()
                .filter(n -> "E14".equals(n.get("postcode").asText()))
                .findFirst().orElseThrow();
        assertThat(e14.get("count").asLong()).isEqualTo(1L);
    }

    @Test
    void malformedJson_shouldRouteToDlqSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> events = Arrays.asList(
                toJson(new PageviewEvent(1L, "SW19", "/home", 1700000000L)),
                "{ not valid json }}}",
                "completely broken"
        );

        DataStream<String> source = env.fromCollection(events);
        OutputTag<String> dlqTag = new OutputTag<>("dlq-messages", TypeInformation.of(String.class));

        PageviewAggregatorJob.wireTopology(source, 1, dlqTag,
                new RawCollectingSink(), new AggCollectingSink(), new DlqCollectingSink());

        env.execute("Integration Test - Malformed JSON");

        assertThat(RawCollectingSink.VALUES).hasSize(3);
        assertThat(DlqCollectingSink.VALUES).hasSize(2);
        assertThat(DlqCollectingSink.VALUES).contains("{ not valid json }}}", "completely broken");
        assertThat(AggCollectingSink.VALUES).hasSize(1);
    }

    @Test
    void invalidTimestamp_shouldRouteToDlqSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> events = Arrays.asList(
                toJson(new PageviewEvent(1L, "SW19", "/home", 1700000000L)),   // valid
                toJson(new PageviewEvent(2L, "E14",  "/about", 1_500_000_000L)), // pre-2020 — DLQ
                toJson(new PageviewEvent(3L, "N1",   "/shop",  (System.currentTimeMillis() / 1000L) + 7_200L)) // far future — DLQ
        );

        DataStream<String> source = env.fromCollection(events);
        OutputTag<String> dlqTag = new OutputTag<>("dlq-messages", TypeInformation.of(String.class));

        PageviewAggregatorJob.wireTopology(source, 1, dlqTag,
                new RawCollectingSink(), new AggCollectingSink(), new DlqCollectingSink());

        env.execute("Integration Test - Invalid Timestamps");

        assertThat(RawCollectingSink.VALUES).hasSize(3);
        assertThat(DlqCollectingSink.VALUES).hasSize(2);
        assertThat(AggCollectingSink.VALUES).hasSize(1);

        JsonNode result = OBJECT_MAPPER.readTree(AggCollectingSink.VALUES.get(0));
        assertThat(result.get("postcode").asText()).isEqualTo("SW19");
        assertThat(result.get("count").asLong()).isEqualTo(1L);
    }

    @Test
    void nullPostcode_shouldRouteToDlqSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> events = Arrays.asList(
                toJson(new PageviewEvent(1L, "SW19", "/home", 1700000000L)),
                toJson(new PageviewEvent(2L, null, "/about", 1700000010L)),
                toJson(new PageviewEvent(3L, "  ", "/contact", 1700000020L))
        );

        DataStream<String> source = env.fromCollection(events);
        OutputTag<String> dlqTag = new OutputTag<>("dlq-messages", TypeInformation.of(String.class));

        PageviewAggregatorJob.wireTopology(source, 1, dlqTag,
                new RawCollectingSink(), new AggCollectingSink(), new DlqCollectingSink());

        env.execute("Integration Test - Null/Blank Postcode");

        assertThat(RawCollectingSink.VALUES).hasSize(3);
        assertThat(DlqCollectingSink.VALUES).hasSize(2);
        assertThat(AggCollectingSink.VALUES).hasSize(1);

        JsonNode result = OBJECT_MAPPER.readTree(AggCollectingSink.VALUES.get(0));
        assertThat(result.get("postcode").asText()).isEqualTo("SW19");
        assertThat(result.get("count").asLong()).isEqualTo(1L);
    }

    private static String toJson(PageviewEvent event) {
        try {
            return OBJECT_MAPPER.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /* flink serializes SinkFunction instances across the minicluster, so instance fields don't survive the round-trip — static lists are the workaround */

    static class RawCollectingSink implements SinkFunction<String> {
        static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value, Context context) {
            VALUES.add(value);
        }
    }

    static class AggCollectingSink implements SinkFunction<String> {
        static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value, Context context) {
            VALUES.add(value);
        }
    }

    static class DlqCollectingSink implements SinkFunction<String> {
        static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value, Context context) {
            VALUES.add(value);
        }
    }
}
