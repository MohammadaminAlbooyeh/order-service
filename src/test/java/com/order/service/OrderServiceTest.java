package com.order.service;

import com.order.messaging.OrderEventProducer;
import com.order.model.Order;
import com.order.model.OrderItem;
import com.order.model.enums.OrderStatus;
import com.order.repository.OrderRepository;
import com.order.saga.OrderSagaOrchestrator;
import com.platform.events.CartCheckoutEvent;
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
class OrderServiceTest {

    @Mock
    OrderRepository orderRepository;

    @Mock
    OrderEventProducer eventProducer;

    @Mock
    OrderSagaOrchestrator sagaOrchestrator;

    @InjectMocks
    OrderService orderService;

    @Test
    void createOrderFromCheckoutWhenNotExists() {
        String orderId = "ord-new";
        CartCheckoutEvent event = CartCheckoutEvent.builder()
                .orderId(orderId)
                .userId("u1")
                .totalAmount(BigDecimal.valueOf(300))
                .items(List.of(
                        CartCheckoutEvent.Item.builder().productId("p1").name("Laptop").unitPrice(BigDecimal.valueOf(100)).quantity(1).build(),
                        CartCheckoutEvent.Item.builder().productId("p2").name("Mouse").unitPrice(BigDecimal.valueOf(200)).quantity(1).build()
                ))
                .build();
        Order saved = buildOrder(orderId, OrderStatus.PENDING, BigDecimal.valueOf(300));
        when(orderRepository.findByOrderId(orderId)).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class))).thenReturn(saved);

        Order result = orderService.createOrderFromCheckout(event);

        assertThat(result.getOrderId()).isEqualTo(orderId);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.getTotalAmount()).isEqualByComparingTo(BigDecimal.valueOf(300));
        verify(eventProducer).publishOrderCreated(eq(orderId), eq("u1"), any(), eq(BigDecimal.valueOf(300)));
    }

    @Test
    void createOrderFromCheckoutWhenAlreadyExists() {
        String orderId = "ord-existing";
        CartCheckoutEvent event = CartCheckoutEvent.builder()
                .orderId(orderId)
                .userId("u1")
                .items(List.of())
                .totalAmount(BigDecimal.valueOf(300))
                .build();
        Order existing = buildOrder(orderId, OrderStatus.CONFIRMED, BigDecimal.valueOf(300));
        when(orderRepository.findByOrderId(orderId)).thenReturn(Optional.of(existing));

        Order result = orderService.createOrderFromCheckout(event);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        verify(orderRepository, never()).save(any());
        verify(eventProducer, never()).publishOrderCreated(any(), any(), any(), any());
    }

    @Test
    void createOrderFromCheckoutComputesTotalFromItemsWhenNull() {
        String orderId = "ord-compute";
        CartCheckoutEvent event = CartCheckoutEvent.builder()
                .orderId(orderId)
                .userId("u1")
                .totalAmount(null)
                .items(List.of(
                        CartCheckoutEvent.Item.builder().productId("p1").unitPrice(BigDecimal.valueOf(100)).quantity(2).build(),
                        CartCheckoutEvent.Item.builder().productId("p2").unitPrice(BigDecimal.valueOf(50)).quantity(2).build()
                ))
                .build();
        when(orderRepository.findByOrderId(orderId)).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Order result = orderService.createOrderFromCheckout(event);

        assertThat(result.getTotalAmount()).isEqualByComparingTo(BigDecimal.valueOf(300));
        verify(eventProducer).publishOrderCreated(eq(orderId), eq("u1"), any(), eq(BigDecimal.valueOf(300)));
    }

    @Test
    void getOrderWhenNotFoundThrowsException() {
        when(orderRepository.findByOrderId("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder("missing"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Order not found: missing");
    }

    @Test
    void getOrderWhenFoundReturnsOrder() {
        Order order = buildOrder("ord-1", OrderStatus.PENDING, BigDecimal.valueOf(100));
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));

        Order result = orderService.getOrder("ord-1");

        assertThat(result.getOrderId()).isEqualTo("ord-1");
    }

    @Test
    void listOrdersReturnsUserOrders() {
        Order order = buildOrder("ord-1", OrderStatus.PENDING, BigDecimal.valueOf(100));
        when(orderRepository.findByUserIdOrderByCreatedAtDesc("u1")).thenReturn(List.of(order));

        List<Order> result = orderService.listOrders("u1");

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOrderId()).isEqualTo("ord-1");
    }

    @Test
    void cancelOrderDelegatesToSagaAndReturnsOrder() {
        Order order = buildOrder("ord-1", OrderStatus.PENDING, BigDecimal.valueOf(100));
        when(orderRepository.findByOrderId("ord-1")).thenReturn(Optional.of(order));

        Order result = orderService.cancelOrder("ord-1", "user request");

        assertThat(result.getOrderId()).isEqualTo("ord-1");
        verify(sagaOrchestrator).cancelOrder("ord-1", "user request");
    }

    private Order buildOrder(String orderId, OrderStatus status, BigDecimal totalAmount) {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setUserId("u1");
        order.setStatus(status);
        order.setTotalAmount(totalAmount);
        order.setItems(List.of());
        return order;
    }
}
