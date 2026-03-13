package com.pipeline.streaming.producer.service;

import com.pipeline.streaming.avro.PageviewEvent;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Random;


@Slf4j
@Service
public class DataGeneratorService {

    private final KafkaTemplate<@NonNull String, @NonNull PageviewEvent> kafkaTemplate;
    private final String topic;
    private final Random random;

    private static final List<String> POSTCODES = List.of("SW19", "E14", "W1A", "EC1A", "N1", "SE1");

    private static final List<String> WEBPAGES = List.of(
            "www.website.com/index.html",
            "www.website.com/products.html",
            "www.website.com/checkout.html",
            "www.website.com/contact.html"
    );

    public DataGeneratorService(
            KafkaTemplate<@NonNull String, @NonNull PageviewEvent> kafkaTemplate,
            @Value("${app.kafka.topic:pageviews-raw}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.random = new Random();
    }

    /* 100k visits/day works out to ~1.15 events/sec; 800ms rate gives ~1.25 — slightly over, intentional headroom */
    @Scheduled(fixedRateString = "${app.generator.rate-ms:800}")
    public void generateAndPublishEvent() {
        try {
            String postcode = POSTCODES.get(random.nextInt(POSTCODES.size()));

            PageviewEvent event = PageviewEvent.newBuilder()
                    .setUserId(random.nextLong(10_000L) + 1L)
                    .setPostcode(postcode)
                    .setWebpage(WEBPAGES.get(random.nextInt(WEBPAGES.size())))
                    .setTimestamp(Instant.now().getEpochSecond())
                    .build();

            /* keying by postcode here means flink's keyBy(postcode) won't cause cross-partition shuffles */
            kafkaTemplate.send(topic, postcode, event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Async send failed for event [userId={}]: {}", event.getUserId(), ex.getMessage());
                        } else {
                            log.info("Published event - '{}'", event);
                        }
                    });

        } catch (Exception ex) {
            log.error("Failed to publish pageview event to Kafka: {}", ex.getMessage());
        }
    }

}
