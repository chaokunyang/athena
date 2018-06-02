package com.timeyang.athena.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public interface RpcEndpoint {

    Logger LOGGER = LoggerFactory.getLogger(RpcEndpoint.class);

    default void onConnected(RpcAddress address) {
        LOGGER.info("Connected to " + address);
    }

    default void onDisconnected(RpcAddress address) {
        LOGGER.info("Disconnected to " + address);
    }

    default void receive(Object msg) {
        LOGGER.info("Received " + msg);
    }

    default Serializable receiveAndReply(Object msg) {
        LOGGER.info("Received " + msg);
        return null;
    }

    default void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
        LOGGER.error("NetworkError on " + remoteAddress);
        cause.printStackTrace();
    }

    default void onStart() {
        LOGGER.info(this + " started");
    }

    default void onStop() {
        LOGGER.info(this + " started");
    }

    default void onError(Throwable cause, RpcAddress remoteAddress) {
        LOGGER.error("Error on " + remoteAddress);
        cause.printStackTrace();
    }

}
