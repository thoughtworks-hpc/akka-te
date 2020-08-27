package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.thoughtworks.hpc.te.controller.Trade;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;

// Todo: 可能由RootActor来创建TradeForwarder不太合适
public class RootActor extends AbstractBehavior<RootActor.CreateTradeForwarder> {
    Logger logger;

    public static Behavior<CreateTradeForwarder> create() {
        return Behaviors.setup(RootActor::new);
    }

    private RootActor(ActorContext<CreateTradeForwarder> context) {
        super(context);
        logger = getContext().getLog();

        List<Integer> symbolIDs = context.getSystem().settings().config().getIntList("te.symbol-id");
        for (int symbolID : symbolIDs) {
            String actorName = "match_actor_" + symbolID;
            context.spawn(MatchActor.create(symbolID), actorName);
            logger.info("Spawn actor {}", actorName);
        }
    }

    public static class CreateTradeForwarder {
        StreamObserver<Trade> responseObserver;

        public CreateTradeForwarder(StreamObserver<Trade> responseObserver) {
            this.responseObserver = responseObserver;
        }
    }

    @Override
    public Receive<CreateTradeForwarder> createReceive() {
        return newReceiveBuilder()
                .onMessage(CreateTradeForwarder.class, this::onCreateTradeForwarder)
                .build();
    }

    private Behavior<CreateTradeForwarder> onCreateTradeForwarder(CreateTradeForwarder createTradeForwarder) {
        String actor_name = "trade_forwarder_" + UUID.randomUUID().toString();
        logger.info("spawn trade forwarder: " + actor_name);
        getContext().spawn(TradeForwarderActor.create(createTradeForwarder.responseObserver), actor_name);
        return this;
    }
}
