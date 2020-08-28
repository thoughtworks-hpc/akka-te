package com.thoughtworks.hpc.te.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.eventstream.EventStream;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.google.protobuf.Timestamp;
import com.thoughtworks.hpc.te.controller.Order;
import com.thoughtworks.hpc.te.controller.Trade;
import com.thoughtworks.hpc.te.controller.TradingSide;
import org.slf4j.Logger;

import java.util.PriorityQueue;

public class MatchActor extends AbstractBehavior<Order> {
    private final Logger logger;
    private PriorityQueue<Order> buyOrderQueue;
    private PriorityQueue<Order> sellOrderQueue;

    private MatchActor(ActorContext<Order> context) {
        super(context);
        logger = getContext().getLog();

        buyOrderQueue = new PriorityQueue<>((o1, o2) -> {
            if (o1.getPrice() != o2.getPrice()) {
                // price DESC
                return o2.getPrice() - o1.getPrice();
            }
            // time ASC
            return (int) (o1.getSubmitTime().getSeconds() - o2.getSubmitTime().getSeconds());
        });
        sellOrderQueue = new PriorityQueue<>(((o1, o2) -> {
            if (o1.getPrice() != o2.getPrice()) {
                // price ASC
                return o1.getPrice() - o2.getPrice();
            }
            // time ASC
            return (int) (o1.getSubmitTime().getSeconds() - o2.getSubmitTime().getSeconds());
        }));
    }

    public static Behavior<Order> create(int symbolId) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(generateServiceKey(symbolId), context.getSelf()));
            return new MatchActor(context);
        });
    }

    public static ServiceKey<Order> generateServiceKey(int symbolId) {
        return ServiceKey.create(Order.class, "symbol_" + symbolId);
    }


    private Behavior<Order> match(Order order) {
        logger.info("MatchActor handle order {}", order);
        // Todo: 撮合逻辑
        Order buy;
        Order sell;

        if (order.getTradingSide() == TradingSide.TRADING_BUY) {
            buy = order;
            sell = sellOrderQueue.peek();
        } else {
            sell = order;
            buy = buyOrderQueue.peek();
        }

        if (buy == null || sell == null) {
            logger.info("Opposite order queue is empty. add order {} to queue.", order.getOrderId());
            addOrderToQueue(order);
            return Behaviors.same();
        }

        if (buy.getPrice() < sell.getPrice()) {
            logger.info("Buy order price [{}] less than sell order[{}].", buy.getPrice(), sell.getPrice());
            addOrderToQueue(order);
            return Behaviors.same();
        }

        // buy price >= sell price
        if (buy.getAmount() == sell.getAmount()) {
            Trade trade = Trade.newBuilder()
                    .setMakerId(order == buy ? sell.getOrderId() : buy.getOrderId())
                    .setTakerId(order.getOrderId())
                    .setTradingSide(order.getTradingSide())
                    .setAmount(order.getAmount())
                    .setPrice(order == buy ? sell.getPrice() : buy.getPrice())
                    .setSellerUserId(sell.getUserId())
                    .setBuyerUserId(buy.getUserId())
                    .setSymbolId(order.getSymbolId())
                    .setDealTime(generateCurrentTimestamp())
                    .build();
            if (order == buy) {
                sellOrderQueue.poll();
            } else {
                buyOrderQueue.poll();
            }
            logger.info("Match success, trade {}", trade);
            getContext().getSystem().eventStream().tell(new EventStream.Publish<>(trade));
            return Behaviors.same();
        }

        return Behaviors.same();
    }

    private Timestamp generateCurrentTimestamp() {
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
    public Receive<Order> createReceive() {
        return newReceiveBuilder().onMessage(Order.class, this::match).build();
    }
}
