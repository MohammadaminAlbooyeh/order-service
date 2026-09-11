package com.order.service;

import com.order.model.Order;
import com.order.model.enums.OrderStatus;
import com.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderExpiryScheduler {

    private static final List<OrderStatus> EXPIRABLE_STATUSES = List.of(
            OrderStatus.PENDING, OrderStatus.INVENTORY_RESERVED, OrderStatus.AWAITING_PAYMENT);

    private final OrderRepository orderRepository;
    private final OrderService orderService;

    @org.springframework.beans.factory.annotation.Value("${order.expiry.ttl-minutes:15}")
    private long orderTtlMinutes = 15;

    @org.springframework.beans.factory.annotation.Value("${order.expiry.enabled:true}")
    private boolean expiryEnabled = true;

    @Scheduled(fixedDelayString = "${order.expiry.poll-interval-ms:60000}")
    public void expireStaleOrders() {
        if (!expiryEnabled) {
            return;
        }
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(orderTtlMinutes);
        List<Order> stale = orderRepository.findByStatusInAndCreatedAtBefore(EXPIRABLE_STATUSES, deadline);
        for (Order order : stale) {
            try {
                log.info("Expiring stale order {}", order.getOrderId());
                orderService.cancelOrder(order.getOrderId(), "Order timed out");
            } catch (Exception e) {
                log.warn("Failed to expire order {}: {}", order.getOrderId(), e.getMessage());
            }
        }
    }
}