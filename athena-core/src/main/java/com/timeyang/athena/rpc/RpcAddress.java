package com.timeyang.athena.rpc;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class RpcAddress implements Serializable {
    private final String host;
    private final int port;

    public RpcAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "RpcAddress{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
