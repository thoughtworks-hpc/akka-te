package com.thoughtworks.hpc.te.domain;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.pubsub.Topic;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.google.protobuf.Timestamp;
import com.thoughtworks.hpc.te.controller.Trade;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;

import java.util.PriorityQueue;

public class MatchActor extends AbstractBehavior<MatchActor.Command> {
    private final Logger logger;
    private final PriorityQueue<Order> buyOrderQueue;
    private final PriorityQueue<Order> sellOrderQueue;
    private final ActorRef<Topic.Command<Trade>> topic;

    public interface Command extends CborSerializable {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static final class MatchOrder implements Command {
        public Order order;
    }

    private MatchActor(ActorContext<Command> context, ActorRef<Topic.Command<Trade>> topic) {
        super(context);
        logger = getContext().getLog();
        this.topic = topic;

        buyOrderQueue = new PriorityQueue<>((o1, o2) -> {
            if (o1.getPrice() != o2.getPrice()) {
                // price DESC
                return o2.getPrice() - o1.getPrice();
            }
            // time ASC
            return (int) (o1.getSubmitTime() - o2.getSubmitTime());
        });
        sellOrderQueue = new PriorityQueue<>(((o1, o2) -> {
            if (o1.getPrice() != o2.getPrice()) {
                // price ASC
                return o1.getPrice() - o2.getPrice();
            }
            // time ASC
            return (int) (o1.getSubmitTime() - o2.getSubmitTime());
        }));
    }

    public static Behavior<Command> create(int symbolId, ActorRef<Topic.Command<Trade>> topic) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(generateServiceKey(symbolId), context.getSelf()));
            return new MatchActor(context, topic);
        });
    }

    public static ServiceKey<Command> generateServiceKey(int symbolId) {
        return ServiceKey.create(Command.class, "symbol_" + symbolId);
    }


    private Behavior<Command> match(MatchOrder matchOrder) {
        Order order = matchOrder.order;
        logger.info("MatchActor handle order {}", order);
        Order buyOrder;
        Order sellOrder;

        if (order.getTradingSide() == TradingSide.TRADING_BUY) {
            buyOrder = order;
            sellOrder = sellOrderQueue.peek();
        } else {
            sellOrder = order;
            buyOrder = buyOrderQueue.peek();
        }

        if (buyOrder == null || sellOrder == null) {
            logger.info("Opposite order queue is empty. add order {} to queue.", order.getOrderId());
            addOrderToQueue(order);
            return Behaviors.same();
        }

        if (buyOrder.getPrice() < sellOrder.getPrice()) {
            logger.info("Buy order price [{}] less than sell order[{}].", buyOrder.getPrice(), sellOrder.getPrice());
            addOrderToQueue(order);
            return Behaviors.same();
        }

        // buy order price >= sell order price
        Trade trade = generateTrade(order, buyOrder, sellOrder, Math.min(buyOrder.getAmount(), sellOrder.getAmount()));
        sendTradeToEventStream(trade);

        if (order == buyOrder) {
            sellOrderQueue.poll();
        } else {
            buyOrderQueue.poll();
        }

        if (buyOrder.getAmount() < sellOrder.getAmount()) {
            Order remainingSellOrder = sellOrder.toBuilder()
                    .amount(sellOrder.getAmount() - buyOrder.getAmount())
                    .build();
            if (order == buyOrder) {
                sellOrderQueue.add(remainingSellOrder);
                return Behaviors.same();
            } else {
                return match(new MatchOrder(remainingSellOrder));
            }
        }

        if (buyOrder.getAmount() > sellOrder.getAmount()) {
            Order remainingBuyOrder = buyOrder.toBuilder()
                    .amount(buyOrder.getAmount() - sellOrder.getAmount())
                    .build();
            if (order == buyOrder) {
                return match(new MatchOrder(remainingBuyOrder));
            } else {
                buyOrderQueue.add(remainingBuyOrder);
                return Behaviors.same();
            }
        }

        return Behaviors.same();
    }

    private void sendTradeToEventStream(Trade trade) {
        logger.info("Match success, trade {}", trade);
        topic.tell(Topic.publish(trade));
    }

    private Trade generateTrade(Order order, Order buyOrder, Order sellOrder, int amount) {
        Order maker = order == buyOrder ? sellOrder : buyOrder;
        com.thoughtworks.hpc.te.controller.TradingSide tradingSide;
        tradingSide = com.thoughtworks.hpc.te.controller.TradingSide.valueOf(order.getTradingSide().toString());
        return Trade.newBuilder()
                .setMakerId(maker.getOrderId())
                .setTakerId(order.getOrderId())
                .setTradingSide(tradingSide)
                .setAmount(amount)
                .setPrice(maker.getPrice())
                .setSellerUserId(sellOrder.getUserId())
                .setBuyerUserId(buyOrder.getUserId())
                .setSymbolId(order.getSymbolId())
                .setDealTime(generateCurrentTimestamp())
                .build();
    }

    public static Timestamp generateCurrentTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
                .setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000_000))
                .build();
    }

    private void addOrderToQueue(Order order) {
        if (order.getTradingSide() == TradingSide.TRADING_BUY) {
            buyOrderQueue.add(order);
        } else {
            sellOrderQueue.add(order);
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder().onMessage(MatchOrder.class, this::match).build();
    }
}