package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.pubsub.Topic;
import com.thoughtworks.hpc.te.controller.Trade;
import io.grpc.stub.StreamObserver;

public class TradeForwarderActor extends AbstractBehavior<Trade> {

    private final StreamObserver<Trade> responseObserver;
    private final ActorRef<Topic.Command<Trade>> topic;

    public static Behavior<Trade> create(StreamObserver<Trade> responseObserver, ActorRef<Topic.Command<Trade>> topic) {
        return Behaviors.setup(context -> new TradeForwarderActor(context, responseObserver, topic));
    }

    private TradeForwarderActor(ActorContext<Trade> context, StreamObserver<Trade> responseObserver, ActorRef<Topic.Command<Trade>> topic) {
        super(context);
        this.responseObserver = responseObserver;
        this.topic = topic;

        topic.tell(Topic.subscribe(getContext().getSelf()));
    }

    @Override
    public Receive<Trade> createReceive() {
        return newReceiveBuilder().onMessage(Trade.class, this::onTrade).build();
    }

    private Behavior<Trade> onTrade(Trade trade) {
        responseObserver.onNext(trade);
        // Todo: 这里需要处理如果客户端断开了的情况
        getContext().getLog().info("Send trade to client");
        return this;
    }

    // Todo: actor挂掉时，需要调用responseObserver.onCompleted()
}
