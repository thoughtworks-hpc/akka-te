package com.thoughtworks.hpc.te.controller;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.cluster.Member;
import akka.cluster.typed.Cluster;
import com.google.protobuf.Empty;
import com.thoughtworks.hpc.te.domain.MatchActor;
import com.thoughtworks.hpc.te.domain.RootActor;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TradingEngineGRPCImpl extends TradingEngineGrpc.TradingEngineImplBase {
    private final ActorSystem<RootActor.Command> system;

    private Map<Integer, ActorRef<MatchActor.Command>> symbolIdToActor;
    private AtomicBoolean isInNoMatchMode;
    private AtomicLong orderCounter;

    public TradingEngineGRPCImpl(ActorSystem<RootActor.Command> system) {
        this.system = system;
        symbolIdToActor = new ConcurrentHashMap<>();
        isInNoMatchMode = new AtomicBoolean(false);
        orderCounter = new AtomicLong(0);
    }

    @Override
    public void match(Order order, StreamObserver<Reply> responseObserver) {
        system.log().debug("GRPC Receive match request " + order);
        orderCounter.incrementAndGet();

        Reply reply = Reply.newBuilder().setStatus(Status.STATUS_SUCCESS).setMessage("ok").build();
        if (isInNoMatchMode.get()) {
            responseObserver.onNext(reply);
            responseObserver.onCompleted();

            return;
        }

        final int symbolId = order.getSymbolId();

        ActorRef<MatchActor.Command> actorOfSymbolId = symbolIdToActor.get(symbolId);
        if (actorOfSymbolId == null) {
            ServiceKey<MatchActor.Command> serviceKey = MatchActor.generateServiceKey(symbolId);

            CompletionStage<Receptionist.Listing> result =
                    AskPattern.ask(
                            system.receptionist(),
                            sendListingTo -> Receptionist.find(serviceKey, sendListingTo),
                            Duration.ofSeconds(5),
                            system.scheduler());

            result.whenComplete(((listing, throwable) -> {
                if (listing != null && listing.isForKey(serviceKey)) {
                    Set<ActorRef<MatchActor.Command>> serviceInstances = listing.getServiceInstances(serviceKey);
                    if (serviceInstances.isEmpty()) {
                        system.log().error("Related match actor not found, symbol_id {}", order.getSymbolId());
                    }
                    serviceInstances.forEach(actor -> {
                        actor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(order)));
                        system.log().info("Cache actor for symbol_id {}", order.getSymbolId());
                        symbolIdToActor.put(symbolId, actor);
                    });
                }

                throw new RuntimeException("not what I wanted");
            }));
        } else {
            actorOfSymbolId.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(order)));
        }


        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void subscribeMatchResult(Empty request, StreamObserver<Trade> responseObserver) {
        Member selfMember = Cluster.get(system).selfMember();
        if (!selfMember.hasRole("gateway")) {
            responseObserver.onError(io.grpc.Status.UNAVAILABLE.withDescription("Current node are not gateway, can not subscribe.").asException());
            return;
        }

        system.tell(new RootActor.CreateTradeForwarder(responseObserver));
    }

    @Override
    public void enterNoMatchMode(com.google.protobuf.Empty request,
                                 io.grpc.stub.StreamObserver<com.thoughtworks.hpc.te.controller.Reply> responseObserver) {
        isInNoMatchMode.set(true);

        Reply reply = Reply.newBuilder().setStatus(Status.STATUS_SUCCESS).setMessage("ok").build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void leaveNoMatchMode(Empty request, StreamObserver<Reply> responseObserver) {
        isInNoMatchMode.set(false);

        Reply reply = Reply.newBuilder().setStatus(Status.STATUS_SUCCESS).setMessage("ok").build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void getStats(com.google.protobuf.Empty request,
                         io.grpc.stub.StreamObserver<com.thoughtworks.hpc.te.controller.Stat> responseObserver) {

        List<CompletableFuture<Object>> futureList = new ArrayList<>();
        symbolIdToActor.forEach((symbolId, actor) -> {
            CompletableFuture<Object> future = AskPattern.ask(actor, replyTo -> new MatchActor.MatchStats(replyTo), Duration.ofSeconds(5),
                    system.scheduler()).toCompletableFuture();
            futureList.add(future);
        });

        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        long processedOrderNumber = 0;
        long generatedTradeNumber = 0;
        long buyQueueSize = 0;
        long sellQueueSize = 0;
        Status status = Status.STATUS_SUCCESS;

        for (CompletableFuture<Object> future : futureList) {
            try {
                processedOrderNumber += ((MatchActor.Stats) future.get()).processedOrderNumber;
                generatedTradeNumber += ((MatchActor.Stats) future.get()).generatedTradeNumber;
                buyQueueSize += ((MatchActor.Stats) future.get()).buyQueueSize;
                sellQueueSize += ((MatchActor.Stats) future.get()).sellQueueSize;
            } catch (InterruptedException | ExecutionException e) {
                status = Status.STATUS_FAILURE;
                system.log().info("getStats error: {}", e.toString());
                e.printStackTrace();
            }
        }

        Stat stat = Stat.newBuilder().setReceivedOrderNumber(orderCounter.longValue())
                .setProcessedOrderNumber(processedOrderNumber).setGeneratedTradeNumber(generatedTradeNumber)
                .setBuyQueueSize(buyQueueSize).setSellQueueSize(sellQueueSize)
                .setStatus(status).build();
        responseObserver.onNext(stat);
        responseObserver.onCompleted();
    }
}
