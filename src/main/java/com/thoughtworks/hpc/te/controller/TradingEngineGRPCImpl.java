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
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class TradingEngineGRPCImpl extends TradingEngineGrpc.TradingEngineImplBase {
    private final ActorSystem<RootActor.Command> system;

    public TradingEngineGRPCImpl(ActorSystem<RootActor.Command> system) {
        this.system = system;
    }

    @Override
    public void match(Order order, StreamObserver<Reply> responseObserver) {
        system.log().info("GRPC Receive match request " + order);
        ServiceKey<MatchActor.Command> serviceKey = MatchActor.generateServiceKey(order.getSymbolId());

        CompletionStage<Receptionist.Listing> result =
                AskPattern.ask(
                        system.receptionist(),
                        sendListingTo -> Receptionist.find(serviceKey, sendListingTo),
                        Duration.ofSeconds(5),
                        system.scheduler());
        Reply reply = Reply.newBuilder().setStatus(Status.STATUS_SUCCESS).setMessage("ok").build();
        result.whenComplete(((listing, throwable) -> {
            if (listing != null && listing.isForKey(serviceKey)) {
                Set<ActorRef<MatchActor.Command>> serviceInstances = listing.getServiceInstances(serviceKey);
                if (serviceInstances.isEmpty()) {
                    system.log().error("Related match actor not found, symbol_id {}", order.getSymbolId());
                }
                serviceInstances.forEach(actor -> actor.tell(new MatchActor.MatchOrder(com.thoughtworks.hpc.te.domain.Order.fromProtobufOrder(order))));
            }

            throw new RuntimeException("not what I wanted");
        }));

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
}
