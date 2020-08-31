package com.thoughtworks.hpc.te.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.eventstream.EventStream;
import akka.actor.typed.pubsub.Topic;
import com.google.protobuf.Timestamp;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

// Todo: 使用domain的Order，而不是protobuf的Order
public class MatchActorTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    TestProbe<Trade> subscriber;
    ActorRef<MatchActor.Command> matchActor;

    @Before
    public void setUp() {
        subscriber = testKit.createTestProbe(Trade.class);
        ActorRef<Topic.Command<Trade>> topic = testKit.spawn(Topic.create(Trade.class, "test-topic"));
        topic.tell(Topic.subscribe(subscriber.getRef()));
        matchActor = testKit.spawn(MatchActor.create(1, topic));
    }

    @Test
    public void should_not_generate_trade_given_sell_queue_empty_when_match_buy_order() {
        Order order = Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).build();

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(order)));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_buy_queue_empty_when_match_sell_order() {
        Order order = Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).build();

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(order)));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_sell_price_grater_then_buy_order_when_math_buy_order() {
        Order sellOrder = Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).setPrice(6).build();
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));
        Order buyOrder = Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).setPrice(5).build();

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_buy_price_less_then_sell_order_when_math_sell_order() {
        Order buyOrder = Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).setPrice(5).build();
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        Order sellOrder = Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).setPrice(6).build();
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_have_same_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_have_same_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_less_than_sell_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_less_than_sell_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_greater_than_sell_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(4);
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, sellOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_greater_than_sell_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(4);
        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(buyOrder)));

        matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(sellOrder)));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, sellOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trades_given_a_set_of_orders_when_match() {
        final int symbolId = 1;
        final int userA = 1;
        final int userB = 2;
        List<Order> orders = new ArrayList<>();

        Timestamp timestamp = MatchActor.generateCurrentTimestamp();

        orders.add(Order.newBuilder()
                .setOrderId(1)
                .setSymbolId(symbolId)
                .setUserId(userA)
                .setTradingSide(TradingSide.TRADING_BUY)
                .setPrice(3)
                .setAmount(10)
                .setSubmitTime(timestamp.toBuilder().setSeconds(timestamp.getSeconds() - 2).build())
                .build());

        orders.add(Order.newBuilder()
                .setOrderId(2)
                .setSymbolId(symbolId)
                .setUserId(userA)
                .setTradingSide(TradingSide.TRADING_BUY)
                .setPrice(5)
                .setAmount(10)
                .setSubmitTime(timestamp)
                .build());

        orders.add(Order.newBuilder()
                .setOrderId(3)
                .setSymbolId(symbolId)
                .setUserId(userA)
                .setTradingSide(TradingSide.TRADING_BUY)
                .setPrice(3)
                .setAmount(10)
                .setSubmitTime(timestamp)
                .build());

        orders.add(Order.newBuilder()
                .setOrderId(4)
                .setSymbolId(symbolId)
                .setUserId(userB)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setPrice(4)
                .setAmount(5)
                .setSubmitTime(timestamp)
                .build());

        orders.add(Order.newBuilder()
                .setOrderId(5)
                .setSymbolId(symbolId)
                .setUserId(userB)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setPrice(4)
                .setAmount(5)
                .setSubmitTime(timestamp)
                .build());

        orders.add(Order.newBuilder()
                .setOrderId(6)
                .setSymbolId(symbolId)
                .setUserId(userB)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setPrice(2)
                .setAmount(20)
                .setSubmitTime(timestamp)
                .build());

        List<Trade> wantTrades = new ArrayList<>();
        wantTrades.add(Trade.newBuilder()
                .setMakerId(2)
                .setTakerId(4)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setAmount(5)
                .setPrice(5)
                .setSellerUserId(userB)
                .setBuyerUserId(userA)
                .setSymbolId(symbolId)
                .build());

        wantTrades.add(Trade.newBuilder()
                .setMakerId(2)
                .setTakerId(5)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setAmount(5)
                .setPrice(5)
                .setSellerUserId(userB)
                .setBuyerUserId(userA)
                .setSymbolId(symbolId)
                .build());

        wantTrades.add(Trade.newBuilder()
                .setMakerId(1)
                .setTakerId(6)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setAmount(10)
                .setPrice(3)
                .setSellerUserId(userB)
                .setBuyerUserId(userA)
                .setSymbolId(symbolId)
                .build());

        wantTrades.add(Trade.newBuilder()
                .setMakerId(3)
                .setTakerId(6)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setAmount(10)
                .setPrice(3)
                .setSellerUserId(userB)
                .setBuyerUserId(userA)
                .setSymbolId(symbolId)
                .build());

        for (Order order : orders) {
            matchActor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(order)));
        }

        for (Trade wantTrade : wantTrades) {
            Trade gotTrade = subscriber.expectMessageClass(Trade.class);
            assertTradeEquals(wantTrade, gotTrade);
        }

    }

    private Trade generateTrade(Order sellOder, Order buyOrder, Order maker, int amount) {
        return Trade.newBuilder()
                .setMakerId(maker.getOrderId())
                .setTakerId(sellOder == maker ? buyOrder.getOrderId() : sellOder.getOrderId())
                .setTradingSide(maker == sellOder ? buyOrder.getTradingSide() : sellOder.getTradingSide())
                .setAmount(amount)
                .setPrice(maker.getPrice())
                .setSellerUserId(sellOder.getUserId())
                .setBuyerUserId(buyOrder.getUserId())
                .setSymbolId(buyOrder.getSymbolId())
                .build();
    }

    private Order generateBuyOrder(int amount) {
        return Order.newBuilder()
                .setOrderId(2)
                .setSymbolId(1)
                .setUserId(2)
                .setTradingSide(TradingSide.TRADING_BUY)
                .setPrice(5)
                .setAmount(amount)
                .build();
    }

    private Order generateSellOrder() {
        return Order.newBuilder()
                .setOrderId(1)
                .setSymbolId(1)
                .setUserId(1)
                .setTradingSide(TradingSide.TRADING_SELL)
                .setPrice(3)
                .setAmount(3)
                .build();
    }

    private void assertTradeEquals(Trade want, Trade got) {
        Trade wantOverwriteDealTime = want.toBuilder().setDealTime(got.getDealTime()).build();
        Assert.assertEquals(wantOverwriteDealTime, got);
    }
}
