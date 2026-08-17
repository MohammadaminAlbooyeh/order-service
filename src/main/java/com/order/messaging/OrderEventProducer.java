package com.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    public static final String ORDER_CREATED_TOPIC = "order.created";
    public static final String ORDER_AWAITING_PAYMENT_TOPIC = "order.awaiting_payment";
    public static final String ORDER_CONFIRMED_TOPIC = "order.confirmed";
    public static final String ORDER_CANCELLED_TOPIC = "order.cancelled";
    public static final String RESERVATION_CANCEL_TOPIC = "inventory.reservation_cancel";

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
        send(ORDER_CREATED_TOPIC, orderId, payload);
    }

    public void publishAwaitingPayment(String orderId, BigDecimal amount, String userId) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "amount", amount,
                "userId", userId
        );
        send(ORDER_AWAITING_PAYMENT_TOPIC, orderId, payload);
    }

    public void publishOrderConfirmed(String orderId, String userId, List<Map<String, Object>> items) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "userId", userId,
                "items", items
        );
        send(ORDER_CONFIRMED_TOPIC, orderId, payload);
    }

    public void publishOrderCancelled(String orderId, String reason) {
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "reason", reason == null ? "" : reason
        );
        send(ORDER_CANCELLED_TOPIC, orderId, payload);
    }

    public void publishReservationCancel(String orderId) {
        Map<String, Object> payload = Map.of("orderId", orderId);
        send(RESERVATION_CANCEL_TOPIC, orderId, payload);
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