package com.timeyang.athena.rpc;

import com.timeyang.athena.AthenaException;

/**
 * @author yangck
 */
public class RpcTimeoutException extends AthenaException {

    public RpcTimeoutException(String message) {
        super(message);
    }
}
