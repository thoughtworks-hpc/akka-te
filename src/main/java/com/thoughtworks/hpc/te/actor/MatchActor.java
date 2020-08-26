package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.thoughtworks.hpc.te.controller.Order;

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
        return Behaviors.same();
    }
}
