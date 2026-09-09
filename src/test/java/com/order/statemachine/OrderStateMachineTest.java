package com.order.statemachine;

import com.order.model.enums.OrderEvent;
import com.order.model.enums.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStateMachineTest {

    private final OrderStateMachine stateMachine = new OrderStateMachine();

    @Test
    void validTransitionsFromPending() {
        assertThat(stateMachine.transition(OrderStatus.PENDING, OrderEvent.INVENTORY_RESERVED))
                .isEqualTo(OrderStatus.INVENTORY_RESERVED);
        assertThat(stateMachine.transition(OrderStatus.PENDING, OrderEvent.INVENTORY_RESERVATION_FAILED))
                .isEqualTo(OrderStatus.CANCELLED);
        assertThat(stateMachine.transition(OrderStatus.PENDING, OrderEvent.FRAUD_FLAGGED))
                .isEqualTo(OrderStatus.CANCELLED);
        assertThat(stateMachine.transition(OrderStatus.PENDING, OrderEvent.CANCELLED))
                .isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void validTransitionsFromInventoryReserved() {
        assertThat(stateMachine.transition(OrderStatus.INVENTORY_RESERVED, OrderEvent.AWAITING_PAYMENT))
                .isEqualTo(OrderStatus.AWAITING_PAYMENT);
        assertThat(stateMachine.transition(OrderStatus.INVENTORY_RESERVED, OrderEvent.FRAUD_FLAGGED))
                .isEqualTo(OrderStatus.CANCELLED);
        assertThat(stateMachine.transition(OrderStatus.INVENTORY_RESERVED, OrderEvent.CANCELLED))
                .isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void validTransitionsFromAwaitingPayment() {
        assertThat(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.PAYMENT_SUCCEEDED))
                .isEqualTo(OrderStatus.CONFIRMED);
        assertThat(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.PAYMENT_FAILED))
                .isEqualTo(OrderStatus.CANCELLED);
        assertThat(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.FRAUD_FLAGGED))
                .isEqualTo(OrderStatus.CANCELLED);
        assertThat(stateMachine.transition(OrderStatus.AWAITING_PAYMENT, OrderEvent.CANCELLED))
                .isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void invalidTransitionThrowsException() {
        assertThatThrownBy(() -> stateMachine.transition(OrderStatus.PENDING, OrderEvent.PAYMENT_SUCCEEDED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invalid transition from PENDING on event PAYMENT_SUCCEEDED");

        assertThatThrownBy(() -> stateMachine.transition(OrderStatus.CONFIRMED, OrderEvent.CANCELLED))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> stateMachine.transition(OrderStatus.CANCELLED, OrderEvent.PAYMENT_SUCCEEDED))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> stateMachine.transition(OrderStatus.PENDING, OrderEvent.ORDER_CREATED))
                .isInstanceOf(IllegalStateException.class);
    }
}
