package com.thoughtworks.hpc.te.domain;

import lombok.*;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class Order implements CborSerializable{
    private long orderId;
    private int symbolId;
    private int userId;
    private TradingSide tradingSide;
    private int amount;
    private int price;
    private long submitTime; // millis

    public static Order fromProtobufOrder(com.thoughtworks.hpc.te.controller.Order protobufOrder) {
        return Order.builder()
                .orderId(protobufOrder.getOrderId())
                .symbolId(protobufOrder.getSymbolId())
                .userId(protobufOrder.getUserId())
                .tradingSide(TradingSide.valueOf(protobufOrder.getTradingSide().toString()))
                .amount(protobufOrder.getAmount())
                .price(protobufOrder.getPrice())
                .submitTime(protobufOrder.getSubmitTime())
                .build();
    }
}
