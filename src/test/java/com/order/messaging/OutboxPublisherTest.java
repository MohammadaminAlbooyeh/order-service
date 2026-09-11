package com.order.messaging;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    OutboxEventRepository repository;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @InjectMocks
    OutboxPublisher publisher;

    @Test
    @SuppressWarnings("unchecked")
    void publishesPendingEventsAndMarksPublished() throws Exception {
        OutboxEvent event = OutboxEvent.builder()
                .id(1L).aggregateId("ord-1").eventType("order.created")
                .topic("order.created").key("ord-1").payload("{}")
                .status(OutboxEvent.OutboxStatus.PENDING).retryCount(0).build();
        when(repository.findPendingForRetry(OutboxEvent.OutboxStatus.PENDING, 5)).thenReturn(List.of(event));
        CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> future =
                CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn((CompletableFuture) future);

        publisher.publishPendingEvents();

        assertThat(event.getStatus()).isEqualTo(OutboxEvent.OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isNotNull();
        verify(repository).save(event);
    }

    @Test
    void failedSendIncrementsRetryAndStaysPending() {
        OutboxEvent event = OutboxEvent.builder()
                .id(2L).aggregateId("ord-2").eventType("order.created")
                .topic("order.created").key("ord-2").payload("{}")
                .status(OutboxEvent.OutboxStatus.PENDING).retryCount(0).build();
        when(repository.findPendingForRetry(OutboxEvent.OutboxStatus.PENDING, 5)).thenReturn(List.of(event));
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn((CompletableFuture) failed);

        publisher.publishPendingEvents();

        assertThat(event.getStatus()).isEqualTo(OutboxEvent.OutboxStatus.PENDING);
        assertThat(event.getRetryCount()).isEqualTo(1);
        verify(repository).save(event);
    }

    @Test
    void failedSendAfterMaxRetriesMarksFailed() {
        OutboxEvent event = OutboxEvent.builder()
                .id(3L).aggregateId("ord-3").eventType("order.created")
                .topic("order.created").key("ord-3").payload("{}")
                .status(OutboxEvent.OutboxStatus.PENDING).retryCount(4).build();
        when(repository.findPendingForRetry(OutboxEvent.OutboxStatus.PENDING, 5)).thenReturn(List.of(event));
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn((CompletableFuture) failed);

        publisher.publishPendingEvents();

        assertThat(event.getStatus()).isEqualTo(OutboxEvent.OutboxStatus.FAILED);
        verify(repository).save(event);
    }

    @Test
    void noPendingEventsDoesNothing() {
        when(repository.findPendingForRetry(any(), any(Integer.class))).thenReturn(List.of());

        publisher.publishPendingEvents();

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }
}
