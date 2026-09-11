package com.order.repository;

import com.order.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {

    @Query("select o from Order o left join fetch o.items where o.orderId = :orderId")
    Optional<Order> findByOrderId(@Param("orderId") String orderId);

    @Query("select o from Order o left join fetch o.items where o.userId = :userId order by o.createdAt desc")
    List<Order> findByUserIdOrderByCreatedAtDesc(@Param("userId") String userId);

    List<Order> findByStatusInAndCreatedAtBefore(List<com.order.model.enums.OrderStatus> statuses,
                                                LocalDateTime deadline);
}