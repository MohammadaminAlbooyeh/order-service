package com.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.topics.PlatformTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void publishOrderCreated(String orderId, String userId, List<Map<String, Object>> items,
                                    BigDecimal totalAmount) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "userId", userId,
                "items", items,
                "totalAmount", totalAmount
        );
        writeToOutbox(PlatformTopics.ORDER_CREATED, orderId, payload);
    }

    @Transactional
    public void publishAwaitingPayment(String orderId, BigDecimal amount, String userId) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "amount", amount,
                "userId", userId
        );
        writeToOutbox(PlatformTopics.ORDER_AWAITING_PAYMENT, orderId, payload);
    }

    @Transactional
    public void publishOrderConfirmed(String orderId, String userId, List<Map<String, Object>> items) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "userId", userId,
                "items", items
        );
        writeToOutbox(PlatformTopics.ORDER_CONFIRMED, orderId, payload);
    }

    @Transactional
    public void publishOrderCancelled(String orderId, String reason) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "reason", reason == null ? "" : reason
        );
        writeToOutbox(PlatformTopics.ORDER_CANCELLED, orderId, payload);
    }

    @Transactional
    public void publishReservationCancel(String orderId) {
        Map<String, Object> payload = Map.of("orderId", orderId);
        writeToOutbox(PlatformTopics.INVENTORY_RESERVATION_CANCEL, orderId, payload);
    }

    private void writeToOutbox(String topic, String key, Map<String, Object> payload) {
        try {
            String payloadJson = objectMapper.writeValueAsString(payload);
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateId(key)
                    .eventType(topic)
                    .topic(topic)
                    .key(key)
                    .payload(payloadJson)
                    .status(OutboxEvent.OutboxStatus.PENDING)
                    .build();
            outboxEventRepository.save(event);
            log.debug("Written outbox event for topic {} key {}", topic, key);
        } catch (Exception e) {
            log.error("Failed to write outbox event for topic {} key {}", topic, key, e);
            throw new IllegalStateException("Failed to write outbox event", e);
        }
    }
}