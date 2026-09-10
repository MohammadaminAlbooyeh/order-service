package com.order.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    @Query("select o from OutboxEvent o where o.status = :status order by o.createdAt asc")
    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(@Param("status") OutboxEvent.OutboxStatus status);

    @Query("select o from OutboxEvent o where o.status = :status and o.retryCount < :maxRetries order by o.createdAt asc")
    List<OutboxEvent> findPendingForRetry(@Param("status") OutboxEvent.OutboxStatus status, @Param("maxRetries") int maxRetries);

    @Query("select o from OutboxEvent o where o.aggregateId = :aggregateId and o.eventType = :eventType and o.status = :status")
    List<OutboxEvent> findByAggregateIdAndEventTypeAndStatus(@Param("aggregateId") String aggregateId,
                                                              @Param("eventType") String eventType,
                                                              @Param("status") OutboxEvent.OutboxStatus status);
}