package com.order.messaging.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FraudFlaggedEvent {

    private String orderId;
    private String riskScore;
    private String reason;
}