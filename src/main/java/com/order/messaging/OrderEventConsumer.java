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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private static final String CONSUMER_GROUP = "order-service";

    private final OrderService orderService;
    private final OrderSagaOrchestrator sagaOrchestrator;
    private final ObjectMapper objectMapper;
    private final IdempotencyService idempotencyService;

    @KafkaListener(topics = PlatformTopics.CART_CHECKOUT, groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory")
    public void onCartCheckout(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processWithIdempotency(record, ack, PlatformTopics.CART_CHECKOUT, message -> {
            CartCheckoutEvent event = parse(message, CartCheckoutEvent.class);
            if (event != null) {
                String eventId = event.getOrderId() + ":checkout";
                if (idempotencyService.tryMarkProcessed(eventId, CONSUMER_GROUP, PlatformTopics.CART_CHECKOUT)) {
                    orderService.createOrderFromCheckout(event);
                } else {
                    log.info("Duplicate CartCheckout event ignored: {}", eventId);
                }
            }
        });
    }

    @KafkaListener(topics = PlatformTopics.INVENTORY_RESERVED, groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory")
    public void onInventoryReserved(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processWithIdempotency(record, ack, PlatformTopics.INVENTORY_RESERVED, message -> {
            InventoryReservedEvent event = parse(message, InventoryReservedEvent.class);
            if (event != null) {
                String reservationId = (event.getReservations() != null && !event.getReservations().isEmpty())
                        ? event.getReservations().get(0).getReservationId()
                        : UUID.randomUUID().toString();
                String eventId = event.getOrderId() + ":reserved:" + reservationId;
                if (idempotencyService.tryMarkProcessed(eventId, CONSUMER_GROUP, PlatformTopics.INVENTORY_RESERVED)) {
                    sagaOrchestrator.onInventoryReserved(event.getOrderId());
                } else {
                    log.info("Duplicate InventoryReserved event ignored: {}", eventId);
                }
            }
        });
    }

    @KafkaListener(topics = PlatformTopics.INVENTORY_RESERVATION_FAILED, groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory")
    public void onInventoryReservationFailed(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processWithIdempotency(record, ack, PlatformTopics.INVENTORY_RESERVATION_FAILED, message -> {
            InventoryReservationFailedEvent event = parse(message, InventoryReservationFailedEvent.class);
            if (event != null) {
                String eventId = event.getOrderId() + ":reservation_failed";
                if (idempotencyService.tryMarkProcessed(eventId, CONSUMER_GROUP, PlatformTopics.INVENTORY_RESERVATION_FAILED)) {
                    sagaOrchestrator.onInventoryReservationFailed(event.getOrderId(), event.getReason());
                } else {
                    log.info("Duplicate InventoryReservationFailed event ignored: {}", eventId);
                }
            }
        });
    }

    @KafkaListener(topics = PlatformTopics.FRAUD_FLAGGED, groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory")
    public void onFraudFlagged(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processWithIdempotency(record, ack, PlatformTopics.FRAUD_FLAGGED, message -> {
            FraudFlaggedEvent event = parse(message, FraudFlaggedEvent.class);
            if (event != null) {
                String eventId = event.getOrderId() + ":fraud";
                if (idempotencyService.tryMarkProcessed(eventId, CONSUMER_GROUP, PlatformTopics.FRAUD_FLAGGED)) {
                    sagaOrchestrator.onFraudFlagged(event.getOrderId(), event.getReason());
                } else {
                    log.info("Duplicate FraudFlagged event ignored: {}", eventId);
                }
            }
        });
    }

    @KafkaListener(topics = PlatformTopics.PAYMENT_SUCCEEDED, groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentSucceeded(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processWithIdempotency(record, ack, PlatformTopics.PAYMENT_SUCCEEDED, message -> {
            PaymentSucceededEvent event = parse(message, PaymentSucceededEvent.class);
            if (event != null) {
                String eventId = event.getOrderId() + ":payment_succeeded:" + event.getTransactionId();
                if (idempotencyService.tryMarkProcessed(eventId, CONSUMER_GROUP, PlatformTopics.PAYMENT_SUCCEEDED)) {
                    sagaOrchestrator.onPaymentSucceeded(event.getOrderId());
                } else {
                    log.info("Duplicate PaymentSucceeded event ignored: {}", eventId);
                }
            }
        });
    }

    @KafkaListener(topics = PlatformTopics.PAYMENT_FAILED, groupId = CONSUMER_GROUP,
            containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentFailed(ConsumerRecord<String, String> record, Acknowledgment ack) {
        processWithIdempotency(record, ack, PlatformTopics.PAYMENT_FAILED, message -> {
            PaymentFailedEvent event = parse(message, PaymentFailedEvent.class);
            if (event != null) {
                String eventId = event.getOrderId() + ":payment_failed:" + event.getTransactionId();
                if (idempotencyService.tryMarkProcessed(eventId, CONSUMER_GROUP, PlatformTopics.PAYMENT_FAILED)) {
                    sagaOrchestrator.onPaymentFailed(event.getOrderId(), event.getReason());
                } else {
                    log.info("Duplicate PaymentFailed event ignored: {}", eventId);
                }
            }
        });
    }

    private void processWithIdempotency(ConsumerRecord<String, String> record, Acknowledgment ack,
                                         String topic, MessageHandler handler) {
        try {
            handler.handle(record.value());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing message from topic {} partition {} offset {}: {}",
                    topic, record.partition(), record.offset(), e.getMessage(), e);
            throw e;
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

    @FunctionalInterface
    private interface MessageHandler {
        void handle(String message);
    }
}