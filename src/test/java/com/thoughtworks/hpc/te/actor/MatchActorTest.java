package com.thoughtworks.hpc.te.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.eventstream.EventStream;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MatchActorTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    TestProbe<Trade> subscriber;
    ActorRef<Order> matchActor;

    @Before
    public void setUp() {
        subscriber = testKit.createTestProbe();
        testKit.system().eventStream().tell(new EventStream.Subscribe<>(Trade.class, subscriber.ref()));
        matchActor = testKit.spawn(MatchActor.create(1));
    }

    @Test
    public void should_not_generate_trade_given_sell_queue_empty_when_match_buy_order() {
        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).build());

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_buy_queue_empty_when_match_sell_order() {
        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).build());

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_sell_price_grater_then_buy_order_when_math_buy_order() {
        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).setPrice(6).build());

        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).setPrice(5).build());

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_buy_price_less_then_sell_order_when_math_sell_order() {
        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_BUY).setPrice(5).build());

        matchActor.tell(Order.newBuilder().setTradingSide(TradingSide.TRADING_SELL).setPrice(6).build());

        subscriber.expectNoMessage();
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_have_same_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(sellOrder);

        matchActor.tell(buyOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_have_same_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(buyOrder);

        matchActor.tell(sellOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_less_than_sell_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(sellOrder);

        matchActor.tell(buyOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_less_than_sell_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(buyOrder);

        matchActor.tell(sellOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_greater_than_sell_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(4);
        matchActor.tell(sellOrder);

        matchActor.tell(buyOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, sellOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_greater_than_sell_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(4);
        matchActor.tell(buyOrder);

        matchActor.tell(sellOrder);

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, sellOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
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
