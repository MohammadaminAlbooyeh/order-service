package com.order.saga;

import com.order.messaging.OrderEventProducer;
import com.order.model.Order;
import com.order.model.enums.OrderEvent;
import com.order.model.enums.OrderStatus;
import com.order.repository.OrderRepository;
import com.order.statemachine.OrderStateMachine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final OrderRepository orderRepository;
    private final OrderStateMachine stateMachine;
    private final CompensationHandler compensationHandler;
    private final OrderEventProducer eventProducer;

    @Transactional
    public void onInventoryReserved(String orderId) {
        Order order = findOrder(orderId);
        if (transition(order, OrderEvent.INVENTORY_RESERVED)) {
            eventProducer.publishAwaitingPayment(order.getOrderId(), order.getTotalAmount(), order.getUserId());
            transition(order, OrderEvent.AWAITING_PAYMENT);
            log.info("Order {} moved to AWAITING_PAYMENT", orderId);
        }
    }

    @Transactional
    public void onInventoryReservationFailed(String orderId, String reason) {
        Order order = findOrder(orderId);
        if (transition(order, OrderEvent.INVENTORY_RESERVATION_FAILED)) {
            eventProducer.publishOrderCancelled(order.getOrderId(), reason);
            log.info("Order {} cancelled: {}", orderId, reason);
        }
    }

    @Transactional
    public void onFraudFlagged(String orderId, String reason) {
        Order order = findOrder(orderId);
        OrderStatus previous = order.getStatus();
        if (transition(order, OrderEvent.FRAUD_FLAGGED)) {
            compensationHandler.compensateIfNeeded(order, previous);
            eventProducer.publishOrderCancelled(order.getOrderId(), "Fraud flagged: " + reason);
            log.info("Order {} cancelled by fraud flag", orderId);
        }
    }

    @Transactional
    public void onPaymentSucceeded(String orderId) {
        Order order = findOrder(orderId);
        if (transition(order, OrderEvent.PAYMENT_SUCCEEDED)) {
            eventProducer.publishOrderConfirmed(order.getOrderId(), order.getUserId(),
                    order.getItems().stream()
                            .map(i -> Map.<String, Object>of(
                                    "productId", i.getProductId(),
                                    "name", i.getName() == null ? "" : i.getName(),
                                    "unitPrice", i.getUnitPrice(),
                                    "quantity", i.getQuantity()))
                            .toList());
            log.info("Order {} CONFIRMED", orderId);
        }
    }

    @Transactional
    public void onPaymentFailed(String orderId, String reason) {
        Order order = findOrder(orderId);
        OrderStatus previous = order.getStatus();
        if (transition(order, OrderEvent.PAYMENT_FAILED)) {
            compensationHandler.compensateIfNeeded(order, previous);
            eventProducer.publishOrderCancelled(order.getOrderId(), "Payment failed: " + reason);
            log.info("Order {} cancelled by payment failure", orderId);
        }
    }

    @Transactional
    public void cancelOrder(String orderId, String reason) {
        Order order = findOrder(orderId);
        if (order.getStatus() == OrderStatus.CONFIRMED) {
            throw new IllegalStateException("Cannot cancel a confirmed order: " + orderId);
        }
        OrderStatus previous = order.getStatus();
        if (transition(order, OrderEvent.CANCELLED)) {
            compensationHandler.compensateIfNeeded(order, previous);
            eventProducer.publishOrderCancelled(order.getOrderId(), reason);
            log.info("Order {} cancelled manually: {}", orderId, reason);
        }
    }

    private Order findOrder(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }

    private boolean transition(Order order, OrderEvent event) {
        try {
            OrderStatus next = stateMachine.transition(order.getStatus(), event);
            order.setStatus(next);
            return true;
        } catch (IllegalStateException e) {
            log.warn("Ignoring {} for order {} in state {} (likely duplicate event)",
                    event, order.getOrderId(), order.getStatus());
            return false;
        }
    }
}