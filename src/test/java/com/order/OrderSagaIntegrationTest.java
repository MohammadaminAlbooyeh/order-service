package com.order;

import com.order.model.Order;
import com.order.model.enums.OrderStatus;
import com.order.repository.OrderRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {
        "cart.checkout",
        "order.created",
        "order.awaiting_payment",
        "order.confirmed",
        "order.cancelled",
        "inventory.reserved",
        "inventory.reservation_failed",
        "inventory.reservation_cancel",
        "fraud.flagged",
        "payment.succeeded",
        "payment.failed"
})
@ActiveProfiles("dev")
class OrderSagaIntegrationTest {

    private static final long TIMEOUT_MS = 15_000;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaTemplate<String, String> producer;

    @Test
    void happyPath() throws Exception {
        String orderId = "ord-happy";

        send("cart.checkout", orderId, checkoutPayload(orderId, 1200));
        assertThat(consume("order.created", orderId)).isNotNull();
        awaitStatus(orderId, OrderStatus.PENDING);

        send("inventory.reserved", orderId,
                "{\"orderId\":\"%s\",\"reservations\":[{\"reservationId\":\"r1\",\"productId\":\"p1\",\"quantity\":1}]}".formatted(orderId));
        assertThat(consume("order.awaiting_payment", orderId)).isNotNull();
        awaitStatus(orderId, OrderStatus.AWAITING_PAYMENT);

        send("payment.succeeded", orderId, "{\"orderId\":\"%s\",\"transactionId\":\"t1\",\"amount\":1200}".formatted(orderId));
        assertThat(consume("order.confirmed", orderId)).isNotNull();
        awaitStatus(orderId, OrderStatus.CONFIRMED);
    }

    @Test
    void inventoryFailureCancelsOrder() throws Exception {
        String orderId = "ord-fail";

        send("cart.checkout", orderId, checkoutPayload(orderId, 50));
        awaitStatus(orderId, OrderStatus.PENDING);

        send("inventory.reservation_failed", orderId,
                "{\"orderId\":\"%s\",\"reason\":\"Insufficient stock\"}".formatted(orderId));
        assertThat(consume("order.cancelled", orderId)).isNotNull();
        awaitStatus(orderId, OrderStatus.CANCELLED);
    }

    @Test
    void fraudFlagCancelsOrderAndCompensates() throws Exception {
        String orderId = "ord-fraud";

        send("cart.checkout", orderId, checkoutPayload(orderId, 500));
        awaitStatus(orderId, OrderStatus.PENDING);

        send("inventory.reserved", orderId,
                "{\"orderId\":\"%s\",\"reservations\":[{\"reservationId\":\"r2\",\"productId\":\"p2\",\"quantity\":1}]}".formatted(orderId));
        awaitStatus(orderId, OrderStatus.AWAITING_PAYMENT);

        send("fraud.flagged", orderId,
                "{\"orderId\":\"%s\",\"riskScore\":\"0.9\",\"reason\":\"suspicious\"}".formatted(orderId));
        assertThat(consume("order.cancelled", orderId)).isNotNull();
        assertThat(consume("inventory.reservation_cancel", orderId)).isNotNull();
        awaitStatus(orderId, OrderStatus.CANCELLED);
    }

    private void send(String topic, String key, String payload) throws Exception {
        producer().send(topic, key, payload).get(5, TimeUnit.SECONDS);
    }

    private KafkaTemplate<String, String> producer() {
        if (producer == null) {
            Map<String, Object> producerProps =
                    KafkaTestUtils.producerProps(embeddedKafkaBroker.getBrokersAsString());
            producerProps.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
            producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
        }
        return producer;
    }

    private ConsumerRecord<String, String> consume(String topic, String key) {
        Map<String, Object> props =
                KafkaTestUtils.consumerProps("test-" + topic, "true", embeddedKafkaBroker);
        props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        try (Consumer<String, String> consumer =
                     new DefaultKafkaConsumerFactory<String, String>(props).createConsumer()) {
            consumer.subscribe(List.of(topic));
            return KafkaTestUtils.getSingleRecord(consumer, topic, java.time.Duration.ofMillis(TIMEOUT_MS));
        }
    }

    private void awaitStatus(String orderId, OrderStatus expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            Order order = orderRepository.findByOrderId(orderId).orElse(null);
            if (order != null && order.getStatus() == expected) {
                return;
            }
            Thread.sleep(100);
        }
        Order order = orderRepository.findByOrderId(orderId).orElse(null);
        assertThat(order).isNotNull();
        assertThat(order.getStatus()).isEqualTo(expected);
    }

    private String checkoutPayload(String orderId, int amount) {
        return "{\"orderId\":\"%s\",\"userId\":\"u1\",\"items\":[{\"productId\":\"p1\",\"name\":\"Laptop\",\"unitPrice\":%d,\"quantity\":1}],\"totalAmount\":%d}"
                .formatted(orderId, amount, amount);
    }
}