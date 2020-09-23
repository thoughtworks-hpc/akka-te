package com.thoughtworks.hpc.te.domain;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.pubsub.Topic;
import com.thoughtworks.hpc.te.controller.Trade;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.UUID;

public class RootActor extends AbstractBehavior<RootActor.Command> {
    private final Logger logger;
    private final ActorRef<Topic.Command<Trade>> topic;

    public interface Command {
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(RootActor::new);
    }

    private RootActor(ActorContext<Command> context) {
        super(context);
        logger = getContext().getLog();

        topic = context.spawn(Topic.create(Trade.class, "topic-trade"), "MyTopic");
        TimeService timeService = new TimeService();

        String symbolIdsStr = context.getSystem().settings().config().getString("te.symbol-id");
        Arrays.stream(symbolIdsStr.split(",")).mapToInt(str -> Integer.parseInt(str.trim())).forEach(symbolId -> {
            String actorName = "match_actor_" + symbolId;
            context.spawn(MatchActor.create(symbolId, topic, timeService), actorName);
            logger.info("Spawn actor {}", actorName);
        });
    }

    public static class CreateTradeForwarder implements Command {
        StreamObserver<Trade> responseObserver;

        public CreateTradeForwarder(StreamObserver<Trade> responseObserver) {
            this.responseObserver = responseObserver;
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(CreateTradeForwarder.class, this::onCreateTradeForwarder)
                .build();
    }

    private Behavior<Command> onCreateTradeForwarder(CreateTradeForwarder createTradeForwarder) {
        String actor_name = "trade_forwarder_" + UUID.randomUUID().toString();
        logger.info("spawn trade forwarder: " + actor_name);
        getContext().spawn(TradeForwarderActor.create(createTradeForwarder.responseObserver, topic), actor_name);
        return this;
    }
}
