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
}
