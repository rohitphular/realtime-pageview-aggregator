package com.pipeline.streaming.producer.service;

import com.pipeline.streaming.producer.model.PageviewEvent;
import lombok.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.util.concurrent.CompletableFuture;


@ExtendWith(MockitoExtension.class)
class DataGeneratorServiceTest {

    @Mock
    private KafkaTemplate<@NonNull String, @NonNull PageviewEvent> kafkaTemplate;

    @Captor
    private ArgumentCaptor<PageviewEvent> eventCaptor;

    @Captor
    private ArgumentCaptor<String> keyCaptor;

    private DataGeneratorService dataGeneratorService;

    @BeforeEach
    void setUp() {
        String testTopic = "test-topic";
        /* kafkaTemplate.send() returns a future; the service chains .whenComplete() on it — must stub with a real completed future */
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        dataGeneratorService = new DataGeneratorService(kafkaTemplate, testTopic);
    }

    @Test
    void shouldGenerateAndPublishValidEvent() {
        dataGeneratorService.generateAndPublishEvent();

        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), eventCaptor.capture());
        verify(kafkaTemplate).send(anyString(), keyCaptor.capture(), any());

        PageviewEvent capturedEvent = eventCaptor.getValue();
        String capturedKey = keyCaptor.getValue();

        assertThat(capturedEvent).isNotNull();
        assertThat(capturedEvent.getUserId()).isBetween(1L, 10_000L);
        assertThat(capturedEvent.getTimestamp()).isPositive();
        assertThat(capturedEvent.getPostcode()).isIn("SW19", "E14", "W1A", "EC1A", "N1", "SE1");
        assertThat(capturedKey).isEqualTo(capturedEvent.getPostcode());
    }

    @Test
    void shouldHandleKafkaSendFailureGracefully() {
        when(kafkaTemplate.send(anyString(), anyString(), any(PageviewEvent.class)))
                .thenThrow(new RuntimeException("Kafka Connection Refused"));

        assertDoesNotThrow(() -> dataGeneratorService.generateAndPublishEvent());
    }

    @RepeatedTest(50)
    void shouldAlwaysGenerateValidDataOverMultipleRuns() {
        ArgumentCaptor<PageviewEvent> eventCaptor = ArgumentCaptor.forClass(PageviewEvent.class);

        dataGeneratorService.generateAndPublishEvent();

        verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString(), eventCaptor.capture());
        PageviewEvent event = eventCaptor.getValue();

        assertThat(event.getUserId()).isBetween(1L, 10_000L);
        assertThat(event.getPostcode()).isNotBlank();
        assertThat(event.getWebpage()).isNotBlank();
        assertThat(event.getTimestamp()).isGreaterThan(0);
    }
}