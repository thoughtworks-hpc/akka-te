package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.pubsub.Topic;
import com.thoughtworks.hpc.te.controller.Trade;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

public class TradeForwarderActor extends AbstractBehavior<Trade> {

    private final StreamObserver<Trade> responseObserver;
    private final Logger logger;

    public static Behavior<Trade> create(StreamObserver<Trade> responseObserver, ActorRef<Topic.Command<Trade>> topic) {
        return Behaviors.setup(context -> new TradeForwarderActor(context, responseObserver, topic));
    }

    private TradeForwarderActor(ActorContext<Trade> context, StreamObserver<Trade> responseObserver, ActorRef<Topic.Command<Trade>> topic) {
        super(context);
        this.responseObserver = responseObserver;
        this.logger = getContext().getLog();

        topic.tell(Topic.subscribe(getContext().getSelf()));
    }

    @Override
    public Receive<Trade> createReceive() {
        return newReceiveBuilder()
                .onMessage(Trade.class, this::onTrade)
                .onSignal(PostStop.class, signal -> onPostStop())
                .build();
    }

    private Behavior<Trade> onPostStop() {
        // close gRPC connection when actor down
        try {
            responseObserver.onCompleted();
        } catch (Exception e) {
            // ignore
            logger.warn("close connection failed. error=" + e);
        }
        return this;
    }

    private Behavior<Trade> onTrade(Trade trade) {
        responseObserver.onNext(trade);
        // Todo: 这里需要处理如果客户端断开了的情况
        logger.info("Send trade {} to client", trade);
        return this;
    }

}
