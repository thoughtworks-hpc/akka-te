package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.ActorContext;

import java.util.List;

public class RootActor extends AbstractBehavior<Void> {
    public static Behavior<Void> create() {
        return Behaviors.setup(RootActor::new);
    }

    private RootActor(ActorContext<Void> context) {
        super(context);

        List<Integer> symbolIDs = context.getSystem().settings().config().getIntList("te.symbol-id");
        for (int symbolID : symbolIDs) {
            String actorName = "match_actor_" + symbolID;
            context.spawn(MatchActor.create(symbolID), actorName);
            getContext().getLog().info("Spawn actor {}", actorName);
        }
    }

    @Override
    public Receive<Void> createReceive() {
        return null;
    }
}
