package com.timeyang.athena.util.cmd;

import com.timeyang.athena.util.IoUtils;
import com.timeyang.athena.util.NetworkUtils;
import com.timeyang.athena.util.StringUtils;
import com.timeyang.athena.util.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

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
                cmd = String.format("'%s'", cmd); // for bash -c in ssh
                String[] commands = {"ssh", host, "bash", "-c", cmd};
                return exec(commands);
            }
        }
    }

    private static Result exec(String[] commands) {
        return exec(commands, true);
    }

    private static Result exec(String[] commands, boolean captureOutput) {
        try {
            LOGGER.info("Execute: " + Arrays.toString(commands));
            Process process = Runtime.getRuntime().exec(commands);

            if (captureOutput) {
                String procOutput = IoUtils.toString(process.getInputStream(), SystemUtils.ENCODING);
                String procError = IoUtils.toString(process.getErrorStream(), SystemUtils.ENCODING);
                int code = process.waitFor();

                if (code != 0) {
                    if (StringUtils.hasLength(procOutput))
                        LOGGER.error("cmd output: " + procOutput);
                    if (StringUtils.hasLength(procError)) {
                        LOGGER.error("cmd error: " + procError);
                    }

                    return new Result(code, procOutput, procError);
                } else {
                    if (StringUtils.hasLength(procOutput))
                        LOGGER.info("cmd output: " + procOutput);
                    if (StringUtils.hasLength(procError)) {
                        LOGGER.info("cmd error: " + procError);
                    }
                    return new Result(code, procOutput, procError);
                }
            } else {
                BufferedReader stdInput = new BufferedReader(new
                        InputStreamReader(process.getInputStream()));

                BufferedReader stdError = new BufferedReader(new
                        InputStreamReader(process.getErrorStream()));

                System.out.println("Standard output of the command:\n");
                String s;
                while ((s = stdInput.readLine()) != null) {
                    System.out.println(s);
                }

                s = stdError.readLine();
                if (s != null) {
                    System.out.println("Standard error of the command:\n");
                    System.out.println(s);
                }
                while ((s = stdError.readLine()) != null) {
                    System.out.println(s);
                }

                int code = process.waitFor();
                return new Result(code, null, null);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return new Result(null, null, null);
        }
    }

}