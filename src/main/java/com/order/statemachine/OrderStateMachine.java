package com.order.statemachine;

import com.order.model.enums.OrderEvent;
import com.order.model.enums.OrderStatus;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

@Component
public class OrderStateMachine {

    private final Map<OrderStatus, Map<OrderEvent, OrderStatus>> transitions;

    public OrderStateMachine() {
        transitions = new EnumMap<>(OrderStatus.class);
        addTransition(OrderStatus.PENDING, OrderEvent.INVENTORY_RESERVED, OrderStatus.INVENTORY_RESERVED);
        addTransition(OrderStatus.PENDING, OrderEvent.INVENTORY_RESERVATION_FAILED, OrderStatus.CANCELLED);
        addTransition(OrderStatus.PENDING, OrderEvent.FRAUD_FLAGGED, OrderStatus.CANCELLED);
        addTransition(OrderStatus.PENDING, OrderEvent.CANCELLED, OrderStatus.CANCELLED);

        addTransition(OrderStatus.INVENTORY_RESERVED, OrderEvent.AWAITING_PAYMENT, OrderStatus.AWAITING_PAYMENT);
        addTransition(OrderStatus.INVENTORY_RESERVED, OrderEvent.FRAUD_FLAGGED, OrderStatus.CANCELLED);
        addTransition(OrderStatus.INVENTORY_RESERVED, OrderEvent.CANCELLED, OrderStatus.CANCELLED);

        addTransition(OrderStatus.AWAITING_PAYMENT, OrderEvent.PAYMENT_SUCCEEDED, OrderStatus.CONFIRMED);
        addTransition(OrderStatus.AWAITING_PAYMENT, OrderEvent.PAYMENT_FAILED, OrderStatus.CANCELLED);
        addTransition(OrderStatus.AWAITING_PAYMENT, OrderEvent.FRAUD_FLAGGED, OrderStatus.CANCELLED);
        addTransition(OrderStatus.AWAITING_PAYMENT, OrderEvent.CANCELLED, OrderStatus.CANCELLED);
    }

    private void addTransition(OrderStatus from, OrderEvent event, OrderStatus to) {
        transitions.computeIfAbsent(from, k -> new EnumMap<>(OrderEvent.class))
                .put(event, to);
    }

    public OrderStatus transition(OrderStatus current, OrderEvent event) {
        Map<OrderEvent, OrderStatus> allowed = transitions.get(current);
        if (allowed == null || !allowed.containsKey(event)) {
            throw new IllegalStateException("Invalid transition from " + current + " on event " + event);
        }
        return allowed.get(event);
    }
}