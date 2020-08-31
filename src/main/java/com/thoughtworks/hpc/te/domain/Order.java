package com.thoughtworks.hpc.te.domain;

public class Order implements CborSerializable{
    private long orderId;
    private int symbolId;
    private int userId;
    private TradingSide tradingSide;
    private int amount;
    private int price;
    private long submitTime; // millis

    public Order() {
    }

    public Order(long orderId, int symbolId, int userId, TradingSide tradingSide, int amount, int price, long submitTime) {
        this.orderId = orderId;
        this.symbolId = symbolId;
        this.userId = userId;
        this.tradingSide = tradingSide;
        this.amount = amount;
        this.price = price;
        this.submitTime = submitTime;
    }

    public static Order fromProtobufOrder(com.thoughtworks.hpc.te.controller.Order protobufOrder) {
        Order order = new Order();

        order.orderId = protobufOrder.getOrderId();
        order.symbolId = protobufOrder.getSymbolId();
        order.userId = protobufOrder.getUserId();
        order.tradingSide = TradingSide.valueOf(protobufOrder.getTradingSide().toString());
        order.amount = protobufOrder.getAmount();
        order.price = protobufOrder.getPrice();
        order.submitTime = protobufOrder.getSubmitTime().getSeconds() * 1000 + protobufOrder.getSubmitTime().getNanos() / 1000_000;

        return order;
    }

    public static Order newWithDifferentAmount(Order order, int newAmount) {
        Order newOrder = new Order();

        newOrder.orderId = order.orderId;
        newOrder.symbolId = order.symbolId;
        newOrder.userId = order.getUserId();
        newOrder.tradingSide = order.tradingSide;
        newOrder.amount = newAmount;
        newOrder.price = order.price;
        newOrder.submitTime = order.submitTime;

        return newOrder;
    }

    // Todo: 使用lombok消除getter，使用lombok使用builder
    public long getOrderId() {
        return orderId;
    }

    public int getSymbolId() {
        return symbolId;
    }

    public int getUserId() {
        return userId;
    }

    public TradingSide getTradingSide() {
        return tradingSide;
    }

    public int getAmount() {
        return amount;
    }

    public int getPrice() {
        return price;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId=" + orderId +
                ", symbolId=" + symbolId +
                ", userId=" + userId +
                ", tradingSide=" + tradingSide +
                ", amount=" + amount +
                ", price=" + price +
                ", submitTime=" + submitTime +
                '}';
    }
}
