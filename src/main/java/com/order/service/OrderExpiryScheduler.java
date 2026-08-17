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

    private static final long ORDER_TTL_MINUTES = 15;

    private final OrderRepository orderRepository;
    private final OrderService orderService;

    @Scheduled(fixedDelay = 60_000)
    @Transactional
    public void expireStaleOrders() {
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(ORDER_TTL_MINUTES);
        List<Order> stale = orderRepository.findAll().stream()
                .filter(o -> o.getCreatedAt().isBefore(deadline))
                .filter(o -> o.getStatus() == OrderStatus.PENDING
                        || o.getStatus() == OrderStatus.INVENTORY_RESERVED
                        || o.getStatus() == OrderStatus.AWAITING_PAYMENT)
                .toList();
        for (Order order : stale) {
            log.info("Expiring stale order {}", order.getOrderId());
            orderService.cancelOrder(order.getOrderId(), "Order timed out");
        }
    }
}