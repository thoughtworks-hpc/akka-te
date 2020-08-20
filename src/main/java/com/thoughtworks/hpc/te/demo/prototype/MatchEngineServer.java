package com.thoughtworks.hpc.te.demo.prototype;


import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class MatchEngineServer {

    private Server server;
    private static ActorSystem<Void> system = null;

    private void start(int port) throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new MatchEngineRPC())
                .build()
                .start();
        System.err.println("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                MatchEngineServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class MatchEngineRPC extends MatchEngineRPCGrpc.MatchEngineRPCImplBase {
        @Override
        public void request(Order request, StreamObserver<Reply> responseObserver) {
            ServiceKey<MatchEngineOfSymbol.Command> key = ServiceKey.create(MatchEngineOfSymbol.Command.class, request.getSymbol());
            CompletionStage<Receptionist.Listing> result =
                    AskPattern.ask(
                            system.receptionist(),
                            sendListingTo -> Receptionist.find(key, sendListingTo),
                            Duration.ofSeconds(3),
                            system.scheduler());
            result.whenComplete(
                    (listing, throwable) -> {
                        if (listing != null && listing.isForKey(key)) {
                            Set<ActorRef<MatchEngineOfSymbol.Command>> serviceInstances = listing.getServiceInstances(key);
                            serviceInstances.forEach(matchEngine -> matchEngine.tell(
                                    new MatchEngineOfSymbol.Order(request.getPrice(), request.getQuantity(), request.getSide())));
                        } else {
                            throw new RuntimeException("not what I wanted");
                        }
                    });
            Reply reply = Reply.newBuilder().setMessage("Ok").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }


    private static class RootBehavior {
        static Behavior<Void> create() {
            return Behaviors.setup(context -> {
                context.spawn(MatchEngineOfSymbol.create(), "MatchEngineOfSymbol");

                return Behaviors.empty();
            });
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int grpcPort;
        if (args.length == 0) {
            startup(25251, "SymbolA");
            startup(25252, "SymbolB");
        } else {
            if (args.length != 3)
                throw new IllegalArgumentException("Usage: rpc_port node_port symbol");
            startup(Integer.parseInt(args[1]), args[2]);
        }
        grpcPort = Integer.parseInt(args[0]);

        final MatchEngineServer server = new MatchEngineServer();
        server.start(grpcPort);
        server.blockUntilShutdown();
    }

    private static void startup(int port, String symbol) {

        // Override the configuration of the port
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("akka.remote.artery.canonical.port", port);
        overrides.put("te.symbol", symbol);

        Config config = ConfigFactory.parseMap(overrides)
                .withFallback(ConfigFactory.load("te"));

        system = ActorSystem.create(RootBehavior.create(), "ClusterSystem", config);
    }

}
