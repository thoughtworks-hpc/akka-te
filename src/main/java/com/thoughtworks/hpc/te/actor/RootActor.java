package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.thoughtworks.hpc.te.controller.Trade;
import io.grpc.stub.StreamObserver;

import java.util.List;

// Todo: 可能由RootActor来创建TradeForwarder不太合适
public class RootActor extends AbstractBehavior<RootActor.CreateTradeForwarder> {
    public static Behavior<CreateTradeForwarder> create() {
        return Behaviors.setup(RootActor::new);
    }

    private RootActor(ActorContext<CreateTradeForwarder> context) {
        super(context);

        List<Integer> symbolIDs = context.getSystem().settings().config().getIntList("te.symbol-id");
        for (int symbolID : symbolIDs) {
            String actorName = "match_actor_" + symbolID;
            context.spawn(MatchActor.create(symbolID), actorName);
            getContext().getLog().info("Spawn actor {}", actorName);
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
        getContext().getLog().info("spawn trade forwarder");
        // todo: 维一的actor_name
        getContext().spawn(TradeForwarderActor.create(createTradeForwarder.responseObserver), "trader_forwarder_1");
        return this;
    }
}
