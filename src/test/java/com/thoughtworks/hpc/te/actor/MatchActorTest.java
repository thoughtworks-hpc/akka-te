package com.thoughtworks.hpc.te.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.eventstream.EventStream;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
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
}
