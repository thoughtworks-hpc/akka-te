package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.google.protobuf.Timestamp;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;

public class MatchActor {

    private final ActorContext<Order> context;

    public static Behavior<Order> create(int symbolId) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(generateServiceKey(symbolId), context.getSelf()));
            return new MatchActor(context).behavior();
        });
    }

    public static ServiceKey<Order> generateServiceKey(int symbolId) {
        return ServiceKey.create(Order.class, "symbol_" + symbolId);
    }

    private MatchActor(ActorContext<Order> context) {
        this.context = context;
    }

    private Behavior<Order> behavior() {
        return Behaviors.receive(Order.class).onMessage(Order.class, this::match).build();
    }

    private Behavior<Order> match(Order order) {
        context.getLog().info("MatchActor handle order {}", order);
        // Todo: 撮合逻辑
        Trade trade = Trade.newBuilder()
                .setMakerId(1)
                .setTakerId(2)
                .setTradingSide(TradingSide.TRADING_BUY)
                .setAmount(3)
                .setPrice(4)
                .setSellerUserId(5)
                .setBuyerUserId(6)
                .setSymbolId(7)
                .setDealTime(Timestamp.getDefaultInstance())
                .build();
        context.getLog().info("Match success, trade {}", trade);
        // Todo: 把成交记录丢给actor，由actor发送给订阅者
        return Behaviors.same();
    }
}
