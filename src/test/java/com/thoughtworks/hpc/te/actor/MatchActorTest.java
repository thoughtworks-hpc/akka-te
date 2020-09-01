package com.thoughtworks.hpc.te.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.pubsub.Topic;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
import com.thoughtworks.hpc.te.domain.MatchActor;
import com.thoughtworks.hpc.te.domain.Order;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.thoughtworks.hpc.te.domain.TradingSide.*;

public class MatchActorTest {
    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    TestProbe<Trade> subscriber;
    ActorRef<Topic.Command<Trade>> topic;
    ActorRef<MatchActor.Command> matchActor;

    @Before
    public void setUp() {
        subscriber = testKit.createTestProbe(Trade.class);
        topic = testKit.spawn(Topic.create(Trade.class, UUID.randomUUID().toString()));
        topic.tell(Topic.subscribe(subscriber.getRef()));
        matchActor = testKit.spawn(MatchActor.create(1, topic));
    }

    @Test
    public void should_not_generate_trade_given_sell_queue_empty_when_match_buy_order() {
        Order order = Order.builder().tradingSide(TRADING_BUY).build();

        matchActor.tell(new MatchActor.MatchOrder(order));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_buy_queue_empty_when_match_sell_order() {
        Order order = Order.builder().tradingSide(TRADING_SELL).build();

        matchActor.tell(new MatchActor.MatchOrder(order));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_sell_price_grater_then_buy_order_when_math_buy_order() {
        Order sellOrder = Order.builder().tradingSide(TRADING_SELL).price(6).build();
        matchActor.tell(new MatchActor.MatchOrder(sellOrder));
        Order buyOrder = Order.builder().tradingSide(TRADING_BUY).price(5).build();

        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_not_generate_trade_given_head_buy_price_less_then_sell_order_when_math_sell_order() {
        Order buyOrder = Order.builder().tradingSide(TRADING_BUY).price(5).build();
        matchActor.tell(new MatchActor.MatchOrder(buyOrder));
        Order sellOrder = Order.builder().tradingSide(TRADING_SELL).price(6).build();

        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

        subscriber.expectNoMessage();
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_have_same_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_have_same_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(3);
        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_less_than_sell_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_less_than_sell_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(2);
        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, buyOrder, buyOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_sell_price_less_than_buy_order_and_buy_amount_greater_than_sell_amount_when_match_buy_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(4);
        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        Trade wantTrade = generateTrade(sellOrder, buyOrder, sellOrder, sellOrder.getAmount());
        Trade gotTrade = subscriber.expectMessageClass(Trade.class);
        assertTradeEquals(wantTrade, gotTrade);
    }

    @Test
    public void should_generate_correct_trade_given_head_buy_price_greater_than_sell_order_and_buy_amount_greater_than_sell_amount_when_match_sell_order() {
        Order sellOrder = generateSellOrder();
        Order buyOrder = generateBuyOrder(4);
        matchActor.tell(new MatchActor.MatchOrder(buyOrder));

        matchActor.tell(new MatchActor.MatchOrder(sellOrder));

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

        long millis = System.currentTimeMillis();

        orders.add(Order.builder()
                .orderId(1)
                .symbolId(symbolId)
                .userId(userA)
                .tradingSide(TRADING_BUY)
                .price(3)
                .amount(10)
                .submitTime(millis - 2)
                .build());

        orders.add(Order.builder()
                .orderId(2)
                .symbolId(symbolId)
                .userId(userA)
                .tradingSide(TRADING_BUY)
                .price(5)
                .amount(10)
                .submitTime(millis)
                .build());

        orders.add(Order.builder()
                .orderId(3)
                .symbolId(symbolId)
                .userId(userA)
                .tradingSide(TRADING_BUY)
                .price(3)
                .amount(10)
                .submitTime(millis)
                .build());

        orders.add(Order.builder()
                .orderId(4)
                .symbolId(symbolId)
                .userId(userB)
                .tradingSide(TRADING_SELL)
                .price(4)
                .amount(5)
                .submitTime(millis)
                .build());

        orders.add(Order.builder()
                .orderId(5)
                .symbolId(symbolId)
                .userId(userB)
                .tradingSide(TRADING_SELL)
                .price(4)
                .amount(5)
                .submitTime(millis)
                .build());

        orders.add(Order.builder()
                .orderId(6)
                .symbolId(symbolId)
                .userId(userB)
                .tradingSide(TRADING_SELL)
                .price(2)
                .amount(20)
                .submitTime(millis)
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
            matchActor.tell(new MatchActor.MatchOrder(order));
        }

        for (Trade wantTrade : wantTrades) {
            Trade gotTrade = subscriber.expectMessageClass(Trade.class);
            assertTradeEquals(wantTrade, gotTrade);
        }

    }

    private Trade generateTrade(Order sellOder, Order buyOrder, Order maker, int amount) {
        com.thoughtworks.hpc.te.domain.TradingSide tradingSide = maker == sellOder ? buyOrder.getTradingSide() : sellOder.getTradingSide();
        return Trade.newBuilder()
                .setMakerId(maker.getOrderId())
                .setTakerId(sellOder == maker ? buyOrder.getOrderId() : sellOder.getOrderId())
                .setTradingSide(TradingSide.valueOf(tradingSide.toString()))
                .setAmount(amount)
                .setPrice(maker.getPrice())
                .setSellerUserId(sellOder.getUserId())
                .setBuyerUserId(buyOrder.getUserId())
                .setSymbolId(buyOrder.getSymbolId())
                .build();
    }

    private Order generateBuyOrder(int amount) {
        return Order.builder()
                .orderId(2)
                .symbolId(1)
                .userId(2)
                .tradingSide(TRADING_BUY)
                .price(5)
                .amount(amount)
                .build();
    }

    private Order generateSellOrder() {
        return Order.builder()
                .orderId(1)
                .symbolId(1)
                .userId(1)
                .tradingSide(TRADING_SELL)
                .price(3)
                .amount(3)
                .build();
    }

    private void assertTradeEquals(Trade want, Trade got) {
        Trade wantOverwriteDealTime = want.toBuilder().setDealTime(got.getDealTime()).build();
        Assert.assertEquals(wantOverwriteDealTime, got);
    }
}
