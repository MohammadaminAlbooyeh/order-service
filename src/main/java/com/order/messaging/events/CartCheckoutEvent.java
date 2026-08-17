package com.order.messaging.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CartCheckoutEvent {

    private String orderId;
    private String userId;
    private List<Item> items;
    private BigDecimal totalAmount;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Item {
        private String productId;
        private String name;
        private BigDecimal unitPrice;
        private Integer quantity;
    }
}