package com.timeyang.athena.utill.cmd;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class Result implements Serializable {
    private boolean succeed;
    private String out;
    private String error;
    private Throwable throwable;

    Result(boolean succeed, Throwable throwable) {
        this.succeed = succeed;
        this.throwable = throwable;
    }

    Result(boolean succeed, String out, String error) {
        this.succeed = succeed;
        this.out = out;
        this.error = error;
    }

    public boolean isSucceed() {
        return succeed;
    }

    public String getOut() {
        return out;
    }

    public String getError() {
        return error;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public String toString() {
        return "Result{" +
                "succeed=" + succeed +
                ", out='" + out + '\'' +
                ", error='" + error + '\'' +
                ", throwable=" + throwable +
                '}';
    }
}
