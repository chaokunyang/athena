package com.timeyang.athena.util.cmd;

import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class Command implements Serializable {
    private final String cmd;

    public Command(String cmd) {
        this.cmd = cmd;
    }

    public Result exec() {
        return CmdUtils.exec(cmd);
    }
}
