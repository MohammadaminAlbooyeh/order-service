package com.order.service;

import com.order.messaging.OrderEventProducer;
import com.order.messaging.events.CartCheckoutEvent;
import com.order.model.Order;
import com.order.model.OrderItem;
import com.order.model.enums.OrderStatus;
import com.order.repository.OrderRepository;
import com.order.saga.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventProducer eventProducer;
    private final OrderSagaOrchestrator sagaOrchestrator;

    @Transactional
    public Order createOrderFromCheckout(CartCheckoutEvent event) {
        if (orderRepository.findByOrderId(event.getOrderId()).isPresent()) {
            log.info("Order {} already exists, ignoring duplicate checkout", event.getOrderId());
            return orderRepository.findByOrderId(event.getOrderId()).orElseThrow();
        }
        Order order = Order.builder()
                .orderId(event.getOrderId())
                .userId(event.getUserId())
                .status(OrderStatus.PENDING)
                .totalAmount(event.getTotalAmount() != null
                        ? event.getTotalAmount()
                        : event.getItems().stream()
                                .map(i -> i.getUnitPrice().multiply(BigDecimal.valueOf(i.getQuantity())))
                                .reduce(BigDecimal.ZERO, BigDecimal::add))
                .build();
        for (CartCheckoutEvent.Item item : event.getItems()) {
            order.addItem(OrderItem.builder()
                    .productId(item.getProductId())
                    .name(item.getName())
                    .unitPrice(item.getUnitPrice())
                    .quantity(item.getQuantity())
                    .build());
        }
        order = orderRepository.save(order);

        eventProducer.publishOrderCreated(order.getOrderId(), order.getUserId(),
                order.getItems().stream()
                        .map(i -> Map.<String, Object>of(
                                "productId", i.getProductId(),
                                "name", i.getName() == null ? "" : i.getName(),
                                "unitPrice", i.getUnitPrice(),
                                "quantity", i.getQuantity()))
                        .toList(),
                order.getTotalAmount());
        log.info("Order {} created (PENDING)", order.getOrderId());
        return order;
    }

    @Transactional
    public Order createOrderDirect(String userId, List<CartCheckoutEvent.Item> items) {
        CartCheckoutEvent event = CartCheckoutEvent.builder()
                .orderId(UUID.randomUUID().toString())
                .userId(userId)
                .items(items)
                .build();
        return createOrderFromCheckout(event);
    }

    @Transactional(readOnly = true)
    public Order getOrder(String orderId) {
        return orderRepository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }

    @Transactional(readOnly = true)
    public List<Order> listOrders(String userId) {
        return orderRepository.findByUserIdOrderByCreatedAtDesc(userId);
    }

    @Transactional
    public Order cancelOrder(String orderId, String reason) {
        sagaOrchestrator.cancelOrder(orderId, reason == null ? "Manually cancelled" : reason);
        return getOrder(orderId);
    }
}