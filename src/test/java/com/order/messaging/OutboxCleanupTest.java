package com.order.messaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxCleanupTest {

    @Mock
    OutboxEventRepository outboxEventRepository;

    @Mock
    ProcessedEventRepository processedEventRepository;

    @InjectMocks
    OutboxCleanup cleanup;

    @Test
    void deletesOldRows() {
        when(outboxEventRepository.deleteByStatusAndPublishedAtBefore(eq(OutboxEvent.OutboxStatus.PUBLISHED), any()))
                .thenReturn(3);
        when(processedEventRepository.deleteByProcessedAtBefore(any())).thenReturn(5);

        cleanup.cleanUp();

        verify(outboxEventRepository).deleteByStatusAndPublishedAtBefore(eq(OutboxEvent.OutboxStatus.PUBLISHED), any());
        verify(processedEventRepository).deleteByProcessedAtBefore(any());
    }

    @Test
    void doesNothingWhenDisabled() {
        ReflectionTestUtils.setField(cleanup, "cleanupEnabled", false);

        cleanup.cleanUp();

        verify(outboxEventRepository, never()).deleteByStatusAndPublishedAtBefore(any(), any());
    }
}
