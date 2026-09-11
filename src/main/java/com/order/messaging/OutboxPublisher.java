package com.order.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    @org.springframework.lang.Nullable
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @org.springframework.beans.factory.annotation.Value("${outbox.batch-size:100}")
    private int batchSize = 100;

    private static final int MAX_RETRIES = 5;

    @jakarta.annotation.PostConstruct
    void registerMetrics() {
        if (meterRegistry != null) {
            meterRegistry.gauge("order.outbox.pending", this,
                    r -> (double) r.outboxEventRepository.count());
        }
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository
                .findPendingForRetry(OutboxEvent.OutboxStatus.PENDING, MAX_RETRIES);

        if (pendingEvents.isEmpty()) {
            return;
        }

        List<OutboxEvent> eventsToProcess = pendingEvents.stream()
                .limit(batchSize)
                .toList();

        for (OutboxEvent event : eventsToProcess) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getKey(), event.getPayload()).get();
                event.setStatus(OutboxEvent.OutboxStatus.PUBLISHED);
                event.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(event);
                log.debug("Published outbox event id {} topic {}", event.getId(), event.getTopic());
            } catch (Exception e) {
                event.setRetryCount(event.getRetryCount() + 1);
                event.setLastError(e.getMessage());
                if (event.getRetryCount() >= MAX_RETRIES) {
                    event.setStatus(OutboxEvent.OutboxStatus.FAILED);
                    log.error("Outbox event id {} failed after {} retries: {}",
                            event.getId(), MAX_RETRIES, e.getMessage());
                } else {
                    log.warn("Outbox event id {} publish failed (attempt {}): {}",
                            event.getId(), event.getRetryCount(), e.getMessage());
                }
                outboxEventRepository.save(event);
            }
        }
    }
}