package com.order.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotencyService {

    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    public boolean tryMarkProcessed(String eventId, String consumerGroup, String topic) {
        if (processedEventRepository.existsByEventIdAndConsumerGroup(eventId, consumerGroup)) {
            log.debug("Duplicate event detected: {} for consumer group {}", eventId, consumerGroup);
            return false;
        }
        ProcessedEvent processedEvent = ProcessedEvent.builder()
                .eventId(eventId)
                .consumerGroup(consumerGroup)
                .topic(topic)
                .build();
        processedEventRepository.save(processedEvent);
        return true;
    }

    public boolean isProcessed(String eventId, String consumerGroup) {
        return processedEventRepository.existsByEventIdAndConsumerGroup(eventId, consumerGroup);
    }
}