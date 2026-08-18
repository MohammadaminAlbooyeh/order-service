package com.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.saga.OrderSagaOrchestrator;
import com.order.service.OrderService;
import com.platform.events.CartCheckoutEvent;
import com.platform.events.FraudFlaggedEvent;
import com.platform.events.InventoryReservationFailedEvent;
import com.platform.events.InventoryReservedEvent;
import com.platform.events.PaymentFailedEvent;
import com.platform.events.PaymentSucceededEvent;
import com.platform.topics.PlatformTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;
    private final OrderSagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = PlatformTopics.CART_CHECKOUT, groupId = "order-service")
    public void onCartCheckout(String message) {
        CartCheckoutEvent event = parse(message, CartCheckoutEvent.class);
        if (event != null) {
            orderService.createOrderFromCheckout(event);
        }
    }

    @KafkaListener(topics = PlatformTopics.INVENTORY_RESERVED, groupId = "order-service")
    public void onInventoryReserved(String message) {
        InventoryReservedEvent event = parse(message, InventoryReservedEvent.class);
        if (event != null) {
            sagaOrchestrator.onInventoryReserved(event.getOrderId());
        }
    }

    @KafkaListener(topics = PlatformTopics.INVENTORY_RESERVATION_FAILED, groupId = "order-service")
    public void onInventoryReservationFailed(String message) {
        InventoryReservationFailedEvent event = parse(message, InventoryReservationFailedEvent.class);
        if (event != null) {
            sagaOrchestrator.onInventoryReservationFailed(event.getOrderId(), event.getReason());
        }
    }

    @KafkaListener(topics = PlatformTopics.FRAUD_FLAGGED, groupId = "order-service")
    public void onFraudFlagged(String message) {
        FraudFlaggedEvent event = parse(message, FraudFlaggedEvent.class);
        if (event != null) {
            sagaOrchestrator.onFraudFlagged(event.getOrderId(), event.getReason());
        }
    }

    @KafkaListener(topics = PlatformTopics.PAYMENT_SUCCEEDED, groupId = "order-service")
    public void onPaymentSucceeded(String message) {
        PaymentSucceededEvent event = parse(message, PaymentSucceededEvent.class);
        if (event != null) {
            sagaOrchestrator.onPaymentSucceeded(event.getOrderId());
        }
    }

    @KafkaListener(topics = PlatformTopics.PAYMENT_FAILED, groupId = "order-service")
    public void onPaymentFailed(String message) {
        PaymentFailedEvent event = parse(message, PaymentFailedEvent.class);
        if (event != null) {
            sagaOrchestrator.onPaymentFailed(event.getOrderId(), event.getReason());
        }
    }

    private <T> T parse(String message, Class<T> type) {
        try {
            return objectMapper.readValue(message, type);
        } catch (Exception e) {
            log.error("Failed to parse Kafka message as {}", type.getSimpleName(), e);
            return null;
        }
    }
}