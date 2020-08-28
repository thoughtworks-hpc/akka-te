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

        // Todo: 重构下面这三段重复代码
        // Todo: order中没有撮合成交的部分继续撮合
        // buy order price >= sell order price
        if (buyOrder.getAmount() == sellOrder.getAmount()) {
            Trade trade = generateTrade(order, buyOrder, sellOrder, order.getAmount());
            if (order == buyOrder) {
                sellOrderQueue.poll();
            } else {
                buyOrderQueue.poll();
            }
            sendTradeToEventStream(trade);
            return Behaviors.same();
        }

        if (buyOrder.getAmount() < sellOrder.getAmount()) {
            Trade trade = generateTrade(order, buyOrder, sellOrder, buyOrder.getAmount());
            if (order == buyOrder) {
                sellOrderQueue.poll();
                Order newSellOrder = sellOrder.toBuilder()
                        .setAmount(sellOrder.getAmount() - buyOrder.getAmount())
                        .build();
                sellOrderQueue.add(newSellOrder);
            } else {
                buyOrderQueue.poll();
            }
            sendTradeToEventStream(trade);
            return Behaviors.same();
        }

        // buy order amount > sell order amount
        Trade trade = generateTrade(order, buyOrder, sellOrder, sellOrder.getAmount());
        if (order == buyOrder) {
            sellOrderQueue.poll();
        } else {
            buyOrderQueue.poll();
            Order newBuyOrder = buyOrder.toBuilder()
                    .setAmount(buyOrder.getAmount() - sellOrder.getAmount())
                    .build();
            buyOrderQueue.add(newBuyOrder);
        }
        sendTradeToEventStream(trade);
        return Behaviors.same();
    }

    private void sendTradeToEventStream(Trade trade) {
        logger.info("Match success, trade {}", trade);
        getContext().getSystem().eventStream().tell(new EventStream.Publish<>(trade));
    }

    private Trade generateTrade(Order order, Order buyOrder, Order sellOrder, int amount) {
        Order maker = order == buyOrder ? sellOrder : buyOrder;
        return Trade.newBuilder()
                .setMakerId(maker.getOrderId())
                .setTakerId(order.getOrderId())
                .setTradingSide(order.getTradingSide())
                .setAmount(amount)
                .setPrice(maker.getPrice())
                .setSellerUserId(sellOrder.getUserId())
                .setBuyerUserId(buyOrder.getUserId())
                .setSymbolId(order.getSymbolId())
                .setDealTime(generateCurrentTimestamp())
                .build();
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
