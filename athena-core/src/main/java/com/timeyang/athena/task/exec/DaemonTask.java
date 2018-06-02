package com.timeyang.athena.task.exec;

import com.timeyang.athena.AthenaException;
import com.timeyang.athena.rpc.RpcAddress;
import com.timeyang.athena.rpc.RpcEndpoint;
import com.timeyang.athena.rpc.RpcEnv;
import com.timeyang.athena.utill.IoUtils;
import com.timeyang.athena.utill.cmd.CmdUtils;
import com.timeyang.athena.utill.cmd.Command;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static com.timeyang.athena.task.message.TaskMessage.*;

/**
 * Daemon task in nodes
 *
 * @author https://github.com/chaokunyang
 */
public class DaemonTask implements Task, RpcEndpoint {

    @Override
    public void exec(TaskContext ctx) {
        RpcEnv rpcEnv = new RpcEnv("0.0.0.0", 0, this);
        rpcEnv.startAndWait();
    }

    @Override
    public void receive(Object msg) {
        if (msg instanceof BashMessage) {
            String script = ((BashMessage) msg).getScript();
            String fileName = UUID.randomUUID().toString();
            try {
                IoUtils.writeFile(script, fileName);
                String cmd = String.format("chmod +x %s && %s", fileName, fileName);
                CmdUtils.exec(cmd);

                // TODO send TaskSuccess message to TaskManager
            } catch (IOException e) {
                e.printStackTrace();
                try {
                    Files.deleteIfExists(Paths.get(fileName));
                } catch (IOException e1) {
                    e1.printStackTrace();
                    throw new RuntimeException(e1);
                }
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Serializable receiveAndReply(Object msg) {
        if (msg instanceof Command) {
            return ((Command)msg).exec();
        }
        return null;
    }

    @Override
    public void onError(Throwable cause, RpcAddress remoteAddress) {
        throw new AthenaException("An exception occurred when handle message for " + remoteAddress, cause);
    }

}
