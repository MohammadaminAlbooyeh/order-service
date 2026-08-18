package com.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.topics.PlatformTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishOrderCreated(String orderId, String userId, List<Map<String, Object>> items,
                                    BigDecimal totalAmount) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "userId", userId,
                "items", items,
                "totalAmount", totalAmount
        );
        send(PlatformTopics.ORDER_CREATED, orderId, payload);
    }

    public void publishAwaitingPayment(String orderId, BigDecimal amount, String userId) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "amount", amount,
                "userId", userId
        );
        send(PlatformTopics.ORDER_AWAITING_PAYMENT, orderId, payload);
    }

    public void publishOrderConfirmed(String orderId, String userId, List<Map<String, Object>> items) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "userId", userId,
                "items", items
        );
        send(PlatformTopics.ORDER_CONFIRMED, orderId, payload);
    }

    public void publishOrderCancelled(String orderId, String reason) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "reason", reason == null ? "" : reason
        );
        send(PlatformTopics.ORDER_CANCELLED, orderId, payload);
    }

    public void publishReservationCancel(String orderId) {
        Map<String, Object> payload = Map.of("orderId", orderId);
        send(PlatformTopics.INVENTORY_RESERVATION_CANCEL, orderId, payload);
    }

    private void send(String topic, String key, Map<String, Object> payload) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(payload));
            log.info("Published {} for order {}", topic, key);
        } catch (Exception e) {
            log.error("Failed to publish {} for order {}", topic, key, e);
        }
    }
}