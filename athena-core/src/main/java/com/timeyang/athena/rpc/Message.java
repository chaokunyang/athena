package com.timeyang.athena.rpc;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public abstract class Message implements Serializable {

    public static class OnStart extends Message {}

    public static class OnStop extends Message {}

    public static class OneWayOutboxMessage extends Message {
        private final Serializable msg;

        public OneWayOutboxMessage(Serializable msg) {
            this.msg = msg;
        }

        public Serializable getMsg() {
            return msg;
        }
    }

    public static class RpcOutboxMessage extends Message {
        private final Serializable msg;

        public RpcOutboxMessage(Serializable msg) {
            this.msg = msg;
        }

        public Serializable getMsg() {
            return msg;
        }
    }

}
