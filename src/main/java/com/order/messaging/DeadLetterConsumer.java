package com.order.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Observes every {@code *.DLT} topic so dead-lettered messages are not silently buried:
 * each one is logged at ERROR and counted as {@code order.kafka.dlt} (tagged by the
 * originating topic) for alerting.
 */
@Slf4j
@Component
public class DeadLetterConsumer {

    @Nullable
    private final MeterRegistry meterRegistry;

    public DeadLetterConsumer(@Nullable MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topicPattern = "${order.dlt.topic-pattern:.*\\.DLT}", groupId = "order-dlt")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("DEAD LETTER on {} partition {} offset {} key {} value {}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        if (meterRegistry != null) {
            meterRegistry.counter("order.kafka.dlt", "topic", record.topic()).increment();
        }
    }
}
