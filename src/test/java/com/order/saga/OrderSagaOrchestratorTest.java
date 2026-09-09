package com.order.saga;

import com.order.model.Order;
import com.order.model.enums.OrderEvent;
import com.order.model.enums.OrderStatus;
import com.order.messaging.OrderEventProducer;
import com.order.repository.OrderRepository;
import com.order.statemachine.OrderStateMachine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderSagaOrchestratorTest {

    @Mock
    OrderRepository orderRepository;

    @Mock
    OrderStateMachine stateMachine;

    @Mock
    CompensationHandler compensationHandler;

    @Mock
    OrderEventProducer eventProducer;

    @InjectMocks
    OrderSagaOrchestrator sagaOrchestrator;

    @Test
    void onInventoryReservedTransitionsToAwaitingPayment() {
        Order order = buildOrder("ord-1", OrderStatus.PENDING);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.PENDING, OrderEvent.INVENTORY_RESERVED))
                .thenReturn(OrderStatus.INVENTORY_RESERVED);
        when(stateMachine.transition(OrderStatus.INVENTORY_RESERVED, OrderEvent.AWAITING_PAYMENT))
                .thenReturn(OrderStatus.AWAITING_PAYMENT);

        sagaOrchestrator.onInventoryReserved("ord-1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.AWAITING_PAYMENT);
        verify(eventProducer).publishAwaitingPayment("ord-1", order.getTotalAmount(), order.getUserId());
    }

    @Test
    void onInventoryReservationFailedTransitionsToCancelled() {
        Order order = buildOrder("ord-1", OrderStatus.PENDING);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.PENDING, OrderEvent.INVENTORY_RESERVATION_FAILED))
                .thenReturn(OrderStatus.CANCELLED);

        sagaOrchestrator.onInventoryReservationFailed("ord-1", "Insufficient stock");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(eventProducer).publishOrderCancelled("ord-1", "Insufficient stock");
    }

    @Test
    void onFraudFlaggedTransitionsToCancelledAndCompensates() {
        Order order = buildOrder("ord-1", OrderStatus.AWAITING_PAYMENT);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.FRAUD_FLAGGED))
                .thenReturn(OrderStatus.CANCELLED);

        sagaOrchestrator.onFraudFlagged("ord-1", "suspicious");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(compensationHandler).compensateIfNeeded(order, OrderStatus.AWAITING_PAYMENT);
        verify(eventProducer).publishOrderCancelled("ord-1", "Fraud flagged: suspicious");
    }

    @Test
    void onPaymentSucceededTransitionsToConfirmed() {
        Order order = buildOrder("ord-1", OrderStatus.AWAITING_PAYMENT);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.PAYMENT_SUCCEEDED))
                .thenReturn(OrderStatus.CONFIRMED);

        sagaOrchestrator.onPaymentSucceeded("ord-1");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        verify(eventProducer).publishOrderConfirmed(eq("ord-1"), eq(order.getUserId()), any());
    }

    @Test
    void onPaymentFailedTransitionsToCancelledAndCompensates() {
        Order order = buildOrder("ord-1", OrderStatus.AWAITING_PAYMENT);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.PAYMENT_FAILED))
                .thenReturn(OrderStatus.CANCELLED);

        sagaOrchestrator.onPaymentFailed("ord-1", "timeout");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(compensationHandler).compensateIfNeeded(order, OrderStatus.AWAITING_PAYMENT);
        verify(eventProducer).publishOrderCancelled("ord-1", "Payment failed: timeout");
    }

    @Test
    void cancelOrderWhenNotConfirmedCancelsAndCompensates() {
        Order order = buildOrder("ord-1", OrderStatus.PENDING);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.PENDING, OrderEvent.CANCELLED))
                .thenReturn(OrderStatus.CANCELLED);

        sagaOrchestrator.cancelOrder("ord-1", "user request");

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(compensationHandler).compensateIfNeeded(order, OrderStatus.PENDING);
        verify(eventProducer).publishOrderCancelled("ord-1", "user request");
    }

    @Test
    void cancelOrderWhenConfirmedThrowsException() {
        Order order = buildOrder("ord-1", OrderStatus.CONFIRMED);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));

        assertThatThrownBy(() -> sagaOrchestrator.cancelOrder("ord-1", "user request"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot cancel a confirmed order");

        verify(eventProducer, never()).publishOrderCancelled(any(), any());
        verify(compensationHandler, never()).compensateIfNeeded(any(), any());
    }

    @Test
    void onInventoryReservedWhenInvalidTransitionDoesNothing() {
        Order order = buildOrder("ord-1", OrderStatus.CONFIRMED);
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));
        when(stateMachine.transition(OrderStatus.CONFIRMED, OrderEvent.INVENTORY_RESERVED))
                .thenThrow(new IllegalStateException("Invalid transition"));

        sagaOrchestrator.onInventoryReserved("ord-1");

        verify(eventProducer, never()).publishAwaitingPayment(any(), any(), any());
    }

    private Order buildOrder(String orderId, OrderStatus status) {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setUserId("u1");
        order.setStatus(status);
        order.setTotalAmount(BigDecimal.valueOf(100));
        order.setItems(List.of());
        return order;
    }
}
