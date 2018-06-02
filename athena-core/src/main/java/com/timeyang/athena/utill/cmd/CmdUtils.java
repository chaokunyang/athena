package com.timeyang.athena.utill.cmd;

import com.timeyang.athena.utill.IoUtils;
import com.timeyang.athena.utill.StringUtils;
import com.timeyang.athena.utill.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * @author https://github.com/chaokunyang
 */
public class CmdUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CmdUtils.class);

    public static Result exec(String cmd) {
        try {
            Process process = Runtime.getRuntime().exec(cmd);
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
            return new Result(false, e);
        }
    }

}
