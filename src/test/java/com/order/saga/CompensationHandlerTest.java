package com.order.saga;

import com.order.model.Order;
import com.order.model.enums.OrderStatus;
import com.order.messaging.OrderEventProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CompensationHandlerTest {

    @Mock
    OrderEventProducer eventProducer;

    @InjectMocks
    CompensationHandler compensationHandler;

    @Test
    void compensateWhenPreviousStatusIsInventoryReserved() {
        Order order = buildOrder(OrderStatus.CANCELLED, "ord-2");
        compensationHandler.compensateIfNeeded(order, OrderStatus.INVENTORY_RESERVED);
        verify(eventProducer).publishReservationCancel("ord-2");
    }

    @Test
    void compensateWhenPreviousStatusIsAwaitingPayment() {
        Order order = buildOrder(OrderStatus.CANCELLED, "ord-3");
        compensationHandler.compensateIfNeeded(order, OrderStatus.AWAITING_PAYMENT);
        verify(eventProducer).publishReservationCancel("ord-3");
    }

    @Test
    void noCompensationWhenPreviousStatusIsPending() {
        Order order = buildOrder(OrderStatus.CANCELLED, "ord-1");
        compensationHandler.compensateIfNeeded(order, OrderStatus.PENDING);
        verify(eventProducer, never()).publishReservationCancel("ord-1");
    }

    @Test
    void noCompensationWhenPreviousStatusIsConfirmed() {
        Order order = buildOrder(OrderStatus.CONFIRMED, "ord-4");
        compensationHandler.compensateIfNeeded(order, OrderStatus.CONFIRMED);
        verify(eventProducer, never()).publishReservationCancel("ord-4");
    }

    @Test
    void noCompensationWhenPreviousStatusIsCancelled() {
        Order order = buildOrder(OrderStatus.CANCELLED, "ord-5");
        compensationHandler.compensateIfNeeded(order, OrderStatus.CANCELLED);
        verify(eventProducer, never()).publishReservationCancel("ord-5");
    }

    @Test
    void noCompensationWhenPreviousStatusIsFailed() {
        Order order = buildOrder(OrderStatus.FAILED, "ord-6");
        compensationHandler.compensateIfNeeded(order, OrderStatus.FAILED);
        verify(eventProducer, never()).publishReservationCancel("ord-6");
    }

    private Order buildOrder(OrderStatus status, String orderId) {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setStatus(status);
        order.setTotalAmount(java.math.BigDecimal.valueOf(100));
        order.setItems(List.of());
        return order;
    }
}
