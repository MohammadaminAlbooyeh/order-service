package com.order.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.messaging.events.CartCheckoutEvent;
import com.order.messaging.events.FraudFlaggedEvent;
import com.order.messaging.events.InventoryReservationFailedEvent;
import com.order.messaging.events.InventoryReservedEvent;
import com.order.messaging.events.PaymentFailedEvent;
import com.order.messaging.events.PaymentSucceededEvent;
import com.order.saga.OrderSagaOrchestrator;
import com.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private static final String CART_CHECKOUT_TOPIC = "cart.checkout";
    private static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved";
    private static final String INVENTORY_RESERVATION_FAILED_TOPIC = "inventory.reservation_failed";
    private static final String FRAUD_FLAGGED_TOPIC = "fraud.flagged";
    private static final String PAYMENT_SUCCEEDED_TOPIC = "payment.succeeded";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    private final OrderService orderService;
    private final OrderSagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = CART_CHECKOUT_TOPIC, groupId = "order-service")
    public void onCartCheckout(String message) {
        CartCheckoutEvent event = parse(message, CartCheckoutEvent.class);
        if (event != null) {
            orderService.createOrderFromCheckout(event);
        }
    }

    @KafkaListener(topics = INVENTORY_RESERVED_TOPIC, groupId = "order-service")
    public void onInventoryReserved(String message) {
        InventoryReservedEvent event = parse(message, InventoryReservedEvent.class);
        if (event != null) {
            sagaOrchestrator.onInventoryReserved(event.getOrderId());
        }
    }

    @KafkaListener(topics = INVENTORY_RESERVATION_FAILED_TOPIC, groupId = "order-service")
    public void onInventoryReservationFailed(String message) {
        InventoryReservationFailedEvent event = parse(message, InventoryReservationFailedEvent.class);
        if (event != null) {
            sagaOrchestrator.onInventoryReservationFailed(event.getOrderId(), event.getReason());
        }
    }

    @KafkaListener(topics = FRAUD_FLAGGED_TOPIC, groupId = "order-service")
    public void onFraudFlagged(String message) {
        FraudFlaggedEvent event = parse(message, FraudFlaggedEvent.class);
        if (event != null) {
            sagaOrchestrator.onFraudFlagged(event.getOrderId(), event.getReason());
        }
    }

    @KafkaListener(topics = PAYMENT_SUCCEEDED_TOPIC, groupId = "order-service")
    public void onPaymentSucceeded(String message) {
        PaymentSucceededEvent event = parse(message, PaymentSucceededEvent.class);
        if (event != null) {
            sagaOrchestrator.onPaymentSucceeded(event.getOrderId());
        }
    }

    @KafkaListener(topics = PAYMENT_FAILED_TOPIC, groupId = "order-service")
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