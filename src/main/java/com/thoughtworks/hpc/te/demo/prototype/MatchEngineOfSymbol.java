package com.thoughtworks.hpc.te.demo.prototype;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;


public class MatchEngineOfSymbol extends AbstractBehavior<MatchEngineOfSymbol.Command> {

    public static ServiceKey<Command> ORDER_SERVICE_KEY;
    private String symbolName;

    interface Command extends CborSerializable {}

    public static final class Order implements Command {
        public final int price;
        public final int quantity;
        public final int side;

        public Order(int price, int quantity, int side) {
            this.price = price;
            this.quantity = quantity;
            this.side = side;
        }
    }

    public static Behavior<Command> create() {
        return Behaviors.setup(MatchEngineOfSymbol::new);
    }

    private MatchEngineOfSymbol(ActorContext<Command> context) {
        super(context);
        symbolName = context.getSystem().settings().config().getString("te.symbol");
        ORDER_SERVICE_KEY = ServiceKey.create(Command.class, symbolName);
        context.getLog().info("Registering myself with receptionist with name: " + symbolName);
        context.getSystem().receptionist().tell(Receptionist.register(ORDER_SERVICE_KEY, context.getSelf().narrow()));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder().onMessage(Order.class, this::onOrder).build();
    }

    private Behavior<Command> onOrder(Order message) {
        getContext().getLog().info("received order in MatchEngine of " + symbolName + ": price=" + message.price + ", quantity=" + message.quantity + ", side=" + message.side);
        return this;
    }

}