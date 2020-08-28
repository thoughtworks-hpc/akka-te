package com.thoughtworks.hpc.te.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.eventstream.EventStream;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class MatchActorTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void should_not_generate_trade_given_sell_queue_empty_when_match_buy_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));

        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).build());

        probe.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_buy_queue_empty_when_match_sell_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));

        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).build());

        probe.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_sell_price_grater_then_buy_order_when_math_buy_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));
        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).setPrice(6).build());

        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).setPrice(5).build());

        probe.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_buy_price_less_then_sell_order_when_math_sell_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));
        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).setPrice(5).build());

        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).setPrice(6).build());

        probe.expectNoMessage();
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_have_same_amount_when_match_buy_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(sellOrder);

        matchActor.tell(buyOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder);
        Trade gotTrade = probe.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_have_same_amount_when_match_sell_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(buyOrder);

        matchActor.tell(sellOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder);
        Trade gotTrade = probe.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_less_than_sell_amount_when_match_buy_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(sellOrder);

        matchActor.tell(buyOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder);
        Trade gotTrade = probe.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
        // Todo: 部分成交，其余部分进入队列，在这里没有测试到
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_less_than_sell_amount_when_match_sell_order() {
        TestProbe<Trade> probe = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, probe.ref()));
        ActorRef<Order> matchActor = testKit.spawn(MatchActor.create(1));
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(buyOrder);

        matchActor.tell(sellOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder);
        Trade gotTrade = probe.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
        // Todo: 部分成交，其余部分进入队列，在这里没有测试到
    }

    private Trade generateTrade(Order sellOder, Order buyOrder, Order maker) {
        return Trade.newBuilder()
                .setMakerId(maker.getOrderId())
                .setTakerId(sellOder == maker ? buyOrder.getOrderId() : sellOder.getOrderId())
                .setTradingSide(maker == sellOder ? buyOrder.getTradingSide() : sellOder.getTradingSide())
                .setAmount(buyOrder.getAmount())
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
