package com.thoughtworks.hpc.te;

import akka.actor.typed.ActorSystem;
import com.thoughtworks.hpc.te.domain.RootActor;
import com.thoughtworks.hpc.te.controller.GRPCServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;

public class AppMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        Config config = ConfigFactory.load();
        final int rpcPort = config.getInt("rpc.port");
        final GRPCServer server = new GRPCServer();
        ActorSystem<RootActor.Command> system = ActorSystem.create(RootActor.create(), "TradingEngine", config);
        server.start(rpcPort, system);

        server.blockUntilShutdown();
    }
}
