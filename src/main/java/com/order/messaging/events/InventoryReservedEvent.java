package com.order.messaging.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryReservedEvent {

    private String orderId;
    private List<Reservation> reservations;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Reservation {
        private String reservationId;
        private String productId;
        private Integer quantity;
    }
}