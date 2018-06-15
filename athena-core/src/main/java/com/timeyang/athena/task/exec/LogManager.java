package com.timeyang.athena.task.exec;

import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.task.TaskInfo;
import com.timeyang.athena.util.NetworkUtils;
import com.timeyang.athena.util.ThreadUtils;
import com.timeyang.athena.util.cmd.CmdUtils;
import com.timeyang.athena.util.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Task log management
 *
 * @author https://github.com/chaokunyang
 */
public class LogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2, ThreadUtils.createThreadFactory("LogManager"));

    public LogManager(AthenaConf athenaConf) {

        File logSaveDir = new File(TaskUtils.getLogSaveDir());
        if (!logSaveDir.exists())
            logSaveDir.mkdirs();
    }

    public Future<Boolean> collect(TaskInfo taskInfo) {
        LOGGER.info("Submit task [{}] log collect task", taskInfo.getTaskId());
        return executorService.schedule(() -> collectTaskLog(taskInfo), 3, TimeUnit.SECONDS);
    }

    private boolean collectTaskLog(TaskInfo taskInfo) {
        long taskId = taskInfo.getTaskId();
        Path remoteLogFilePath = Paths.get(TaskUtils.getTaskLogFilePath(taskId));
        String host = taskInfo.getHost();
        Path taskLogSavePath = Paths.get(TaskUtils.getTaskLogSavePath(taskId));
        String execTaskDir = TaskUtils.getExecTaskDir(taskId);
        Path execPath = Paths.get(execTaskDir);
        try {
            Files.createDirectories(taskLogSavePath.getParent());
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (NetworkUtils.isHostLocal(taskInfo.getHost())) {
            try {
                Files.move(remoteLogFilePath, taskLogSavePath, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.info("task [{}] log collected", taskId);
                Files.deleteIfExists(execPath);
                LOGGER.info("clear task exec dir succeed");
                return true;
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.warn("task [{}] log collect failed", taskId);
                return false;
            }
        }

        if (SystemUtils.IS_WINDOWS) {
            return false;
        } else {
            String cmd = String.format("scp %s:%s %s", host, remoteLogFilePath, taskLogSavePath);
            boolean succeed = CmdUtils.exec(cmd).isSucceed();
            if (succeed) {
                LOGGER.info("collect task [{}] log succeed", taskId);
            } else {
                LOGGER.error("collect task [{}] log failed", taskId);
            }
            String clearCmd = "rm -rf " + execTaskDir;
            CmdUtils.exec(host, clearCmd);
            LOGGER.info("clear task exec dir succeed");

            return succeed;
        }
    }

    public List<String> getLogLines(long taskId, int lineNumber, int rows) {
        String taskLogSavePath = TaskUtils.getTaskLogSavePath(taskId);
        if (!new File(taskLogSavePath).exists())
            return new ArrayList<>();

        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(taskLogSavePath, "r");
            for (int i = 0; i < lineNumber; i++) {
                if (file.readLine() == null)
                    return new ArrayList<>();
            }

            List<String> lines = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                String line = file.readLine();
                if (line != null) {
                    lines.add(line);
                } else {
                    return lines;
                }
            }
            return lines;
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        } finally {
            if (file != null) {
                try {
                    file.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Check whether is collected or need to be collected
     */
    private void check() {

    }
}
