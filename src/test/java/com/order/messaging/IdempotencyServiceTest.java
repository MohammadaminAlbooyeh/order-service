package com.order.messaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    @Mock
    ProcessedEventRepository repository;

    @InjectMocks
    IdempotencyService service;

    @Test
    void firstEventIsMarkedProcessed() {
        when(repository.existsByEventIdAndConsumerGroup("e1", "order-service")).thenReturn(false);

        assertThat(service.tryMarkProcessed("e1", "order-service", "cart.checkout")).isTrue();
        verify(repository).save(any(ProcessedEvent.class));
    }

    @Test
    void duplicateEventIsRejected() {
        when(repository.existsByEventIdAndConsumerGroup("e1", "order-service")).thenReturn(true);

        assertThat(service.tryMarkProcessed("e1", "order-service", "cart.checkout")).isFalse();
        verify(repository, never()).save(any());
    }
}
