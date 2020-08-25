package com.thoughtworks.hpc.te.controller;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class TradingEngineGRPCImpl extends TradingEngineGrpc.TradingEngineImplBase {
    private static final Logger logger = Logger.getLogger(TradingEngineGRPCImpl.class.getName());

    @Override
    public void match(Order order, StreamObserver<Reply> responseObserver) {
        logger.info("Receive match request " + order);
        Reply reply = Reply.newBuilder().setStatus(Status.STATUS_SUCCESS).setMessage("ok").build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void subscribeMatchResult(Empty request, StreamObserver<Trade> responseObserver) {
        // Todo:
    }
}
