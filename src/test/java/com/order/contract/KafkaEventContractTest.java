package com.order.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaEventContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void cartCheckoutEvent_shouldHaveValidSchema() {
        String event = cartCheckoutEvent("order-123", "user-456");
        JsonNode json = parse(event);

        assertThat(json.get("eventId").asText()).isEqualTo("order-123:checkout");
        assertThat(json.get("eventType").asText()).isEqualTo("cart.checkout");
        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("userId").asText()).isEqualTo("user-456");
        assertThat(json.get("totalAmount").asDouble()).isEqualTo(20.00);

        ArrayNode items = (ArrayNode) json.get("items");
        assertThat(items.size()).isEqualTo(1);
        assertThat(items.get(0).get("productId").asText()).isEqualTo("prod-1");
        assertThat(items.get(0).get("quantity").asInt()).isEqualTo(2);
    }

    @Test
    void inventoryReservedEvent_shouldHaveValidSchema() {
        String event = inventoryReservedEvent("order-123");
        JsonNode json = parse(event);

        assertThat(json.get("eventId").asText()).isEqualTo("order-123:reserved:res-1");
        assertThat(json.get("eventType").asText()).isEqualTo("inventory.reserved");
        assertThat(json.get("orderId").asText()).isEqualTo("order-123");

        ArrayNode reservations = (ArrayNode) json.get("reservations");
        assertThat(reservations.size()).isEqualTo(1);
        assertThat(reservations.get(0).get("reservationId").asText()).isEqualTo("res-1");
        assertThat(reservations.get(0).get("quantity").asInt()).isEqualTo(2);
    }

    @Test
    void inventoryReservationFailedEvent_shouldHaveValidSchema() {
        String event = inventoryReservationFailedEvent("order-123", "Out of stock");
        JsonNode json = parse(event);

        assertThat(json.get("eventId").asText()).isEqualTo("order-123:reservation_failed");
        assertThat(json.get("eventType").asText()).isEqualTo("inventory.reservation_failed");
        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("reason").asText()).isEqualTo("Out of stock");
    }

    @Test
    void fraudFlaggedEvent_shouldHaveValidSchema() {
        String event = fraudFlaggedEvent("order-123", "Suspicious activity");
        JsonNode json = parse(event);

        assertThat(json.get("eventId").asText()).isEqualTo("order-123:fraud");
        assertThat(json.get("eventType").asText()).isEqualTo("fraud.flagged");
        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("reason").asText()).isEqualTo("Suspicious activity");
    }

    @Test
    void paymentSucceededEvent_shouldHaveValidSchema() {
        String event = paymentSucceededEvent("order-123", "txn-789");
        JsonNode json = parse(event);

        assertThat(json.get("eventId").asText()).isEqualTo("order-123:payment_succeeded:txn-789");
        assertThat(json.get("eventType").asText()).isEqualTo("payment.succeeded");
        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("transactionId").asText()).isEqualTo("txn-789");
        assertThat(json.get("amount").asDouble()).isEqualTo(20.00);
    }

    @Test
    void paymentFailedEvent_shouldHaveValidSchema() {
        String event = paymentFailedEvent("order-123", "txn-789", "Insufficient funds");
        JsonNode json = parse(event);

        assertThat(json.get("eventId").asText()).isEqualTo("order-123:payment_failed:txn-789");
        assertThat(json.get("eventType").asText()).isEqualTo("payment.failed");
        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("transactionId").asText()).isEqualTo("txn-789");
        assertThat(json.get("reason").asText()).isEqualTo("Insufficient funds");
    }

    @Test
    void orderCreatedEvent_shouldHaveValidSchema() {
        String event = orderCreatedEvent("order-123", "user-456");
        JsonNode json = parse(event);

        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("userId").asText()).isEqualTo("user-456");
        assertThat(json.get("totalAmount").asDouble()).isEqualTo(20.00);

        ArrayNode items = (ArrayNode) json.get("items");
        assertThat(items.size()).isEqualTo(1);
    }

    @Test
    void orderAwaitingPaymentEvent_shouldHaveValidSchema() {
        String event = orderAwaitingPaymentEvent("order-123", "user-456");
        JsonNode json = parse(event);

        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("userId").asText()).isEqualTo("user-456");
        assertThat(json.get("amount").asDouble()).isEqualTo(20.00);
    }

    @Test
    void orderConfirmedEvent_shouldHaveValidSchema() {
        String event = orderConfirmedEvent("order-123", "user-456");
        JsonNode json = parse(event);

        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("userId").asText()).isEqualTo("user-456");

        ArrayNode items = (ArrayNode) json.get("items");
        assertThat(items.size()).isEqualTo(1);
    }

    @Test
    void orderCancelledEvent_shouldHaveValidSchema() {
        String event = orderCancelledEvent("order-123", "Customer requested");
        JsonNode json = parse(event);

        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
        assertThat(json.get("reason").asText()).isEqualTo("Customer requested");
    }

    @Test
    void inventoryReservationCancelEvent_shouldHaveValidSchema() {
        String event = inventoryReservationCancelEvent("order-123");
        JsonNode json = parse(event);

        assertThat(json.get("orderId").asText()).isEqualTo("order-123");
    }

    private JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String cartCheckoutEvent(String orderId, String userId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", orderId + ":checkout");
        node.put("eventType", "cart.checkout");
        node.put("timestamp", "2024-01-01T00:00:00Z");
        node.put("orderId", orderId);
        node.put("userId", userId);
        ArrayNode items = node.putArray("items");
        ObjectNode item = items.addObject();
        item.put("productId", "prod-1");
        item.put("name", "Product 1");
        item.put("unitPrice", 10.00);
        item.put("quantity", 2);
        node.put("totalAmount", 20.00);
        return node.toString();
    }

    private String inventoryReservedEvent(String orderId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", orderId + ":reserved:res-1");
        node.put("eventType", "inventory.reserved");
        node.put("timestamp", "2024-01-01T00:00:00Z");
        node.put("orderId", orderId);
        ArrayNode reservations = node.putArray("reservations");
        ObjectNode res = reservations.addObject();
        res.put("reservationId", "res-1");
        res.put("productId", "prod-1");
        res.put("quantity", 2);
        return node.toString();
    }

    private String inventoryReservationFailedEvent(String orderId, String reason) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", orderId + ":reservation_failed");
        node.put("eventType", "inventory.reservation_failed");
        node.put("timestamp", "2024-01-01T00:00:00Z");
        node.put("orderId", orderId);
        node.put("reason", reason);
        return node.toString();
    }

    private String fraudFlaggedEvent(String orderId, String reason) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", orderId + ":fraud");
        node.put("eventType", "fraud.flagged");
        node.put("timestamp", "2024-01-01T00:00:00Z");
        node.put("orderId", orderId);
        node.put("reason", reason);
        return node.toString();
    }

    private String paymentSucceededEvent(String orderId, String transactionId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", orderId + ":payment_succeeded:" + transactionId);
        node.put("eventType", "payment.succeeded");
        node.put("timestamp", "2024-01-01T00:00:00Z");
        node.put("orderId", orderId);
        node.put("transactionId", transactionId);
        node.put("amount", 20.00);
        return node.toString();
    }

    private String paymentFailedEvent(String orderId, String transactionId, String reason) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("eventId", orderId + ":payment_failed:" + transactionId);
        node.put("eventType", "payment.failed");
        node.put("timestamp", "2024-01-01T00:00:00Z");
        node.put("orderId", orderId);
        node.put("transactionId", transactionId);
        node.put("reason", reason);
        return node.toString();
    }

    private String orderCreatedEvent(String orderId, String userId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("orderId", orderId);
        node.put("userId", userId);
        ArrayNode items = node.putArray("items");
        ObjectNode item = items.addObject();
        item.put("productId", "prod-1");
        item.put("name", "Product 1");
        item.put("unitPrice", 10.00);
        item.put("quantity", 2);
        node.put("totalAmount", 20.00);
        return node.toString();
    }

    private String orderAwaitingPaymentEvent(String orderId, String userId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("orderId", orderId);
        node.put("userId", userId);
        node.put("amount", 20.00);
        return node.toString();
    }

    private String orderConfirmedEvent(String orderId, String userId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("orderId", orderId);
        node.put("userId", userId);
        ArrayNode items = node.putArray("items");
        ObjectNode item = items.addObject();
        item.put("productId", "prod-1");
        item.put("name", "Product 1");
        item.put("unitPrice", 10.00);
        item.put("quantity", 2);
        return node.toString();
    }

    private String orderCancelledEvent(String orderId, String reason) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("orderId", orderId);
        node.put("reason", reason);
        return node.toString();
    }

    private String inventoryReservationCancelEvent(String orderId) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("orderId", orderId);
        return node.toString();
    }
}