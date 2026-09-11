package com.order.service;

import com.order.model.Order;
import com.order.model.enums.OrderStatus;
import com.order.repository.OrderRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderExpirySchedulerTest {

    @Mock
    OrderRepository orderRepository;

    @Mock
    OrderService orderService;

    @InjectMocks
    OrderExpiryScheduler scheduler;

    @Test
    void expiresStaleOrders() {
        Order stale = buildOrder("ord-stale", OrderStatus.PENDING);
        when(orderRepository.findByStatusInAndCreatedAtBefore(any(), any())).thenReturn(List.of(stale));

        scheduler.expireStaleOrders();

        verify(orderService).cancelOrder("ord-stale", "Order timed out");
    }

    @Test
    void continuesWhenOneExpiryFails() {
        Order bad = buildOrder("ord-bad", OrderStatus.PENDING);
        Order good = buildOrder("ord-good", OrderStatus.AWAITING_PAYMENT);
        when(orderRepository.findByStatusInAndCreatedAtBefore(any(), any()))
                .thenReturn(List.of(bad, good));
        doThrow(new RuntimeException("boom")).when(orderService).cancelOrder(eq("ord-bad"), any());

        scheduler.expireStaleOrders();

        verify(orderService).cancelOrder("ord-good", "Order timed out");
    }

    @Test
    void doesNothingWhenDisabled() {
        ReflectionTestUtils.setField(scheduler, "expiryEnabled", false);

        scheduler.expireStaleOrders();

        verify(orderRepository, never()).findByStatusInAndCreatedAtBefore(any(), any());
    }

    @Test
    void doesNothingWhenNoStaleOrders() {
        when(orderRepository.findByStatusInAndCreatedAtBefore(any(), any())).thenReturn(List.of());

        scheduler.expireStaleOrders();

        verify(orderService, never()).cancelOrder(any(), any());
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
