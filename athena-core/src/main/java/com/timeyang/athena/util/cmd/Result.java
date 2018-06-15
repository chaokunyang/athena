package com.timeyang.athena.util.cmd;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class Result implements Serializable {
    private Integer exitCode;
    private String out;
    private String error;
    private Throwable throwable;

    Result(Integer exitCode, Throwable throwable) {
        this.exitCode = exitCode;
        this.throwable = throwable;
    }

    Result(Integer exitCode, String out, String error) {
        this.exitCode = exitCode;
        this.out = out;
        this.error = error;
    }

    public Integer getExitCode() {
        return exitCode;
    }

    public boolean isSucceed() {
        return exitCode != null && exitCode == 0;
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
                "exitCode=" + exitCode +
                ", succeed=" + isSucceed() +
                ", out='" + out + '\'' +
                ", error='" + error + '\'' +
                ", throwable=" + throwable +
                '}';
    }
}
