package com.order.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Bounds table growth: deletes published outbox rows and old idempotency keys.
 * Retention is configurable; cleanup runs daily by default and is a no-op when disabled.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxCleanup {

    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;

    @Value("${outbox.cleanup.published-retention-days:7}")
    private int publishedRetentionDays = 7;

    @Value("${outbox.cleanup.processed-retention-days:30}")
    private int processedRetentionDays = 30;

    @Value("${outbox.cleanup.enabled:true}")
    private boolean cleanupEnabled = true;

    @Scheduled(fixedDelayString = "${outbox.cleanup.poll-interval-ms:86400000}")
    @Transactional
    public void cleanUp() {
        if (!cleanupEnabled) {
            return;
        }
        int deletedOutbox = outboxEventRepository.deleteByStatusAndPublishedAtBefore(
                OutboxEvent.OutboxStatus.PUBLISHED,
                LocalDateTime.now().minusDays(publishedRetentionDays));
        int deletedProcessed = processedEventRepository.deleteByProcessedAtBefore(
                LocalDateTime.now().minusDays(processedRetentionDays));
        if (deletedOutbox > 0 || deletedProcessed > 0) {
            log.info("Cleanup deleted {} outbox rows and {} processed-event rows", deletedOutbox, deletedProcessed);
        }
    }
}
