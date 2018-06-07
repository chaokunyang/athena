package com.timeyang.athena.utill.cmd;

import com.timeyang.athena.utill.IoUtils;
import com.timeyang.athena.utill.NetworkUtils;
import com.timeyang.athena.utill.StringUtils;
import com.timeyang.athena.utill.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author https://github.com/chaokunyang
 */
public class CmdUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CmdUtils.class);

    public static Result exec(String cmd) {
        String[] commands;
        if (SystemUtils.IS_WINDOWS) {
            commands = new String[]{"cmd", "/c", cmd};
        } else {
            cmd = String.format("'%s'", cmd); // for bash -c
            commands = new String[]{"bash", "-c", cmd};
        }

        return exec(commands);
    }

    public static Result exec(String host, String cmd) {
        if (NetworkUtils.isHostLocal(host)) {
            return exec(cmd);
        } else {
            if (SystemUtils.IS_WINDOWS) {
                String msg = String.format("Execute command on other host in windows is not supported. host [%s], command [%s]", host, cmd);
                throw new UnsupportedOperationException(msg);
            } else {
                cmd = String.format("'%s'", cmd); // for bash -c
                String[] commands = {"ssh", host, "bash", "-c", cmd};
                return exec(commands);
            }
        }
    }

    private static Result exec(String[] commands) {
        try {
            Process process = Runtime.getRuntime().exec(commands);
            int code = process.waitFor();
            String procOutput = IoUtils.toString(process.getInputStream(), SystemUtils.ENCODING);
            String procError = IoUtils.toString(process.getErrorStream(), SystemUtils.ENCODING);

            if (code != 0) {
                if (StringUtils.hasLength(procOutput))
                    LOGGER.error("cmd output: " + procOutput);
                if (StringUtils.hasLength(procError)) {
                    LOGGER.error("cmd error: " + procError);
                }

                return new Result(false, procOutput, procError);
            } else {
                if (StringUtils.hasLength(procOutput))
                    LOGGER.info("cmd output: " + procOutput);
                if (StringUtils.hasLength(procError)) {
                    LOGGER.info("cmd error: " + procError);
                }
                return new Result(true, procOutput, procError);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return new Result(false, null, null);
        }
    }

}