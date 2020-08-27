package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.eventstream.EventStream;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.thoughtworks.hpc.te.controller.Trade;
import io.grpc.stub.StreamObserver;

public class TradeForwarderActor extends AbstractBehavior<Trade> {

    private final StreamObserver<Trade> responseObserver;

    public static Behavior<Trade> create(StreamObserver<Trade> responseObserver) {
        return Behaviors.setup(context -> new TradeForwarderActor(context, responseObserver));
    }

    private TradeForwarderActor(ActorContext<Trade> context, StreamObserver<Trade> responseObserver) {
        super(context);
        this.responseObserver = responseObserver;

        context.getSystem().eventStream().tell(new EventStream.Subscribe<>(Trade.class, getContext().getSelf()));
    }

    @Override
    public Receive<Trade> createReceive() {
        return newReceiveBuilder().onMessage(Trade.class, this::onTrade).build();
    }

    private Behavior<Trade> onTrade(Trade trade) {
        responseObserver.onNext(trade);
        getContext().getLog().info("Send trade to client");
        return this;
    }

    // Todo: actor挂掉时，需要调用responseObserver.onCompleted()
}
