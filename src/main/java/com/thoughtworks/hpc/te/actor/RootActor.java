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
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;

public class RootActor extends AbstractBehavior<RootActor.Command> {
    private final Logger logger;
    private final ActorRef<Topic.Command<Trade>> topic;

    public interface Command {}

    public static Behavior<Command> create() {
        return Behaviors.setup(RootActor::new);
    }

    private RootActor(ActorContext<Command> context) {
        super(context);
        logger = getContext().getLog();

         topic = context.spawn(Topic.create(Trade.class, "topic-trade"), "MyTopic");

        List<Integer> symbolIDs = context.getSystem().settings().config().getIntList("te.symbol-id");
        for (int symbolID : symbolIDs) {
            String actorName = "match_actor_" + symbolID;
            context.spawn(MatchActor.create(symbolID, topic), actorName);
            logger.info("Spawn actor {}", actorName);
        }
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
