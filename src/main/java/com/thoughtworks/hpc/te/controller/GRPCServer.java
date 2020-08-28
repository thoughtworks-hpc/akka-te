package com.thoughtworks.hpc.te.controller;

import akka.actor.typed.ActorSystem;
import com.thoughtworks.hpc.te.actor.RootActor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code GRPCServer}.
 */
public class GRPCServer {
    private static final Logger logger = Logger.getLogger(GRPCServer.class.getName());

    private Server server;

    public void start(int port, ActorSystem<RootActor.Command> system) throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new TradingEngineGRPCImpl(system))
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();
        system.log().info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    GRPCServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
