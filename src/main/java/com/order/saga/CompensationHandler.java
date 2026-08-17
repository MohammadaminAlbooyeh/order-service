package com.order.saga;

import com.order.messaging.OrderEventProducer;
import com.order.model.Order;
import com.order.model.enums.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class CompensationHandler {

    private static final Set<OrderStatus> RESERVATION_HOLDING_STATES =
            Set.of(OrderStatus.INVENTORY_RESERVED, OrderStatus.AWAITING_PAYMENT);

    private final OrderEventProducer eventProducer;

    public void compensateIfNeeded(Order order, OrderStatus previousStatus) {
        if (RESERVATION_HOLDING_STATES.contains(previousStatus)) {
            log.info("Compensating order {}: releasing inventory reservations", order.getOrderId());
            eventProducer.publishReservationCancel(order.getOrderId());
        }
    }
}