package com.pipeline.streaming.processor.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class WindowResultFormatterTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private WindowResultFormatter formatter;

    @Mock
    private WindowResultFormatter.Context context;

    @Mock
    private Collector<String> collector;

    @Captor
    private ArgumentCaptor<String> outputCaptor;

    @BeforeEach
    void setUp() {
        formatter = new WindowResultFormatter();
    }

    @Test
    void shouldFormatOutputAsValidJson() throws Exception {
        TimeWindow window = new TimeWindow(1704067200000L, 1704067260000L);
        when(context.window()).thenReturn(window);

        formatter.process("SW19", context, Collections.singletonList(42L), collector);

        org.mockito.Mockito.verify(collector).collect(outputCaptor.capture());
        String output = outputCaptor.getValue();

        JsonNode json = objectMapper.readTree(output);
        assertThat(json.get("postcode").asText()).isEqualTo("SW19");
        assertThat(json.get("window_start").asText()).isEqualTo("2024-01-01T00:00:00Z");
        assertThat(json.get("window_end").asText()).isEqualTo("2024-01-01T00:01:00Z");
        assertThat(json.get("count").asLong()).isEqualTo(42L);
    }

    @Test
    void shouldIncludeBothWindowBoundaries() throws Exception {
        /* 5-minute window */
        TimeWindow window = new TimeWindow(1704067200000L, 1704067500000L);
        when(context.window()).thenReturn(window);

        formatter.process("E14", context, Collections.singletonList(100L), collector);

        org.mockito.Mockito.verify(collector).collect(outputCaptor.capture());
        JsonNode json = objectMapper.readTree(outputCaptor.getValue());

        assertThat(json.get("window_start").asText()).isNotEqualTo(json.get("window_end").asText());
        assertThat(json.get("window_end").asText()).isEqualTo("2024-01-01T00:05:00Z");
    }
}
