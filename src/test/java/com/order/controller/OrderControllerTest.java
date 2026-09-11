package com.order.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.exception.GlobalExceptionHandler;
import com.order.model.Order;
import com.order.model.enums.OrderStatus;
import com.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    OrderService orderService;

    MockMvc mockMvc;
    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new OrderController(orderService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void createOrderReturns201() throws Exception {
        Order order = buildOrder("ord-1", OrderStatus.PENDING);
        when(orderService.createOrderDirect(eq("u1"), any())).thenReturn(order);

        String body = """
                {"userId":"u1","items":[{"productId":"p1","name":"Laptop","unitPrice":100,"quantity":1}]}""";

        mockMvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value("ord-1"));
    }

    @Test
    void createOrderValidationFailsWhenUserIdMissing() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"\",\"items\":[]}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getOrderReturnsOrder() throws Exception {
        when(orderService.getOrder("ord-1")).thenReturn(buildOrder("ord-1", OrderStatus.PENDING));

        mockMvc.perform(get("/api/orders/ord-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value("ord-1"));
    }

    @Test
    void getOrderNotFoundReturns400() throws Exception {
        when(orderService.getOrder("missing")).thenThrow(new IllegalArgumentException("Order not found: missing"));

        mockMvc.perform(get("/api/orders/missing"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    void listOrdersReturnsPageEnvelope() throws Exception {
        Order order = buildOrder("ord-1", OrderStatus.PENDING);
        when(orderService.listOrders(eq("u1"), any()))
                .thenReturn(new PageImpl<>(List.of(order), PageRequest.of(0, 20), 1));

        mockMvc.perform(get("/api/orders").param("userId", "u1").param("page", "0").param("size", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].orderId").value("ord-1"))
                .andExpect(jsonPath("$.totalElements").value(1));
    }

    @Test
    void cancelOrderReturnsOrder() throws Exception {
        when(orderService.cancelOrder(eq("ord-1"), any())).thenReturn(buildOrder("ord-1", OrderStatus.CANCELLED));

        mockMvc.perform(post("/api/orders/ord-1/cancel")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(Map.of("reason", "user request"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void cancelConfirmedOrderReturns409() throws Exception {
        when(orderService.cancelOrder(eq("ord-1"), any()))
                .thenThrow(new IllegalStateException("Cannot cancel a confirmed order: ord-1"));

        mockMvc.perform(post("/api/orders/ord-1/cancel")
                        .contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isConflict());
    }

    private Order buildOrder(String orderId, OrderStatus status) {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setUserId("u1");
        order.setStatus(status);
        order.setTotalAmount(BigDecimal.valueOf(100));
        order.setItems(List.of());
        return order;
    }
}
