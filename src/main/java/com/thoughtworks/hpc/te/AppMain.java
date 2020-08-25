package com.thoughtworks.hpc.te;

import com.thoughtworks.hpc.te.controller.GRPCServer;

import java.io.IOException;

public class AppMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        final GRPCServer server = new GRPCServer();
        server.start();



        server.blockUntilShutdown();
    }
}
