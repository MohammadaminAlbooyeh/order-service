package com.order.messaging;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {

    @Query("select p from ProcessedEvent p where p.eventId = :eventId and p.consumerGroup = :consumerGroup")
    Optional<ProcessedEvent> findByEventIdAndConsumerGroup(@Param("eventId") String eventId,
                                                            @Param("consumerGroup") String consumerGroup);

    boolean existsByEventIdAndConsumerGroup(String eventId, String consumerGroup);

    @org.springframework.data.jpa.repository.Modifying
    @Query("delete from ProcessedEvent p where p.processedAt < :cutoff")
    int deleteByProcessedAtBefore(@Param("cutoff") java.time.LocalDateTime cutoff);
}