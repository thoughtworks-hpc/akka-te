package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.eventstream.EventStream;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.google.protobuf.Timestamp;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
import org.slf4j.Logger;

public class MatchActor extends AbstractBehavior<Order> {
    private Logger logger;

    private MatchActor(ActorContext<Order> context) {
        super(context);
        logger = getContext().getLog();
    }

    public static Behavior<Order> create(int symbolId) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(generateServiceKey(symbolId), context.getSelf()));
            return new MatchActor(context);
        });
    }

    public static ServiceKey<Order> generateServiceKey(int symbolId) {
        return ServiceKey.create(Order.class, "symbol_" + symbolId);
    }


    private Behavior<Order> match(Order order) {
        logger.info("MatchActor handle order {}", order);
        // Todo: 撮合逻辑
        return Behaviors.same();
//        Trade trade = Trade.newBuilder()
//                .setMakerId(1)
//                .setTakerId(2)
//                .setTradingSide(TradingSide.TRADING_BUY)
//                .setAmount(3)
//                .setPrice(4)
//                .setSellerUserId(5)
//                .setBuyerUserId(6)
//                .setSymbolId(7)
//                .setDealTime(Timestamp.getDefaultInstance())
//                .build();
//        logger.info("Match success, trade {}", trade);
//
//        getContext().getSystem().eventStream().tell(new EventStream.Publish<>(trade));
//
//        return Behaviors.same();
    }

    @Override
    public Receive<Order> createReceive() {
        return newReceiveBuilder().onMessage(Order.class, this::match).build();
    }
}
