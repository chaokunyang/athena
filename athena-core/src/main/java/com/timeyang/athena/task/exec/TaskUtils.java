package com.timeyang.athena.task.exec;

import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.task.TaskInfo;
import com.timeyang.athena.utill.ParametersUtils;
import com.timeyang.athena.utill.StringUtils;
import com.timeyang.athena.utill.SystemUtils;

import java.io.File;
import java.util.Map;

/**
 * @author https://github.com/chaokunyang
 */
public class TaskUtils {
    private static final String DEFAULT_TASK_EXEC_DIR_NAME = ".tasks";
    private static final String DEFAULT_TASKS_DIR_PATH = getDefaultTasksDir();
    private static final String TASK_LOG_FILE_NAME = "task.log";

    private static final AthenaConf athenaConf = AthenaConf.getConf();

    private static String getDefaultTasksDir() {
        String dir = String.format("%s/%s", SystemUtils.WORK_DIR, DEFAULT_TASK_EXEC_DIR_NAME);
        if (SystemUtils.IS_WINDOWS) {
            return dir.replaceAll("/", "\\\\");
        } else {
            return dir;
        }
    }

    public static String getTasksDir() {
        String tasksDir = athenaConf.get("task.exec.tasks.dir");

        if (tasksDir == null) {
            tasksDir = DEFAULT_TASKS_DIR_PATH;
        } else {
            if (SystemUtils.IS_WINDOWS) {
                tasksDir = tasksDir.replaceAll("/", "\\\\");
            }
            tasksDir = new File(tasksDir).getAbsolutePath();
        }

        return tasksDir;
    }

    public static String getExecTaskDir(long taskId) {
        String tasksDir = getTasksDir();

        String dir = String.format("%s/%d", tasksDir, taskId);
        if (SystemUtils.IS_WINDOWS) {
            return dir.replaceAll("/", "\\\\");
        } else {
            return dir;
        }
    }

    public static String getTaskInitCmd(long taskId) {
        String taskDir = getExecTaskDir(taskId);
        if (SystemUtils.IS_WINDOWS) {
            // must add brackets to "if not exist", or else following commands will bt treated as on command. No need for linux.
            return String.format("cmd /c (if not exist %s mkdir %s)", taskDir, taskDir);
        } else {
            return String.format("[ -d %s ] || mkdir -p %s", taskDir, taskDir);
        }
    }

    public static String getTaskExecCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
        Long taskId = task.getTaskId();
        String params = task.getParams();
        if (!StringUtils.hasText(params)) {
            params = "";
        }

        String classpath = task.getClasspath() + ";" +
                ParametersUtils.fromArgs(params).getOrDefault("extraClasspath", "");
        if (StringUtils.hasText(classpath)) {
            classpath = " -classpath \"" + classpath + "\" ";
        } else {
            classpath = " ";
        }

        params = " --taskId " + taskId
                + " --taskManagerHost " + taskRpcHost
                + " --taskManagerPort " + taskRpcPort
                + " --taskFilePath " + getRemoteTaskLogFilePath(taskId)
                + " " + params;
        String redirectOut = " >" + getExecTaskDir(taskId) + "/" + TASK_LOG_FILE_NAME
                + " 2>&1 &";
        String cmd = "java -server -XX:OnOutOfMemoryError=kill "
                + classpath
                + TaskExecutorLauncher.class.getCanonicalName()
                + params + " "
                + redirectOut;
        if (!isHostLocal(task.getHost())) {
            cmd = "ssh " + task.getHost() + " '" + cmd + "'";
        }
        if (SystemUtils.IS_WINDOWS) {
            cmd = cmd.replaceAll("/", "\\\\");
        }
        if (SystemUtils.isLinux()) {
            cmd = "nohup " + cmd;
        }

        return cmd;
    }

    public static String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
        return task.getTaskType().getTaskCmd(task, taskRpcHost, taskRpcPort);
    }

    /**
     * @return log save parent directory
     */
    public static String getLogSaveDir() {
        String p = athenaConf.get("task.log.save.dir");

        if (p == null) {
            p = DEFAULT_TASKS_DIR_PATH;
        } else {
            if (SystemUtils.IS_WINDOWS) {
                p = p.replaceAll("/", "\\\\");
            }
            p = new File(p).getAbsolutePath();
        }
        return p;
    }

    public static boolean isHostLocal(String host) {
        return  "localhost".equalsIgnoreCase(host) ||
                "127.0.0.1".equalsIgnoreCase(host) ||
                SystemUtils.HOSTNAME.equalsIgnoreCase(host) ||
                SystemUtils.LOCAL_ADDRESSES.contains(host);
    }

    public static String getRemoteTaskLogFilePath(long taskId) {
        String path = getExecTaskDir(taskId) + "/" + TASK_LOG_FILE_NAME;
        if (SystemUtils.IS_WINDOWS) {
            return path.replaceAll("/", "\\\\");
        }
        return path;
    }

    public static String getTaskLogSavePath(long taskId) {
        String path = String.format("%s/%d/%s", getLogSaveDir(), taskId, TASK_LOG_FILE_NAME);
        if (SystemUtils.IS_WINDOWS) {
            return path.replaceAll("/", "\\\\");
        }
        return path;
    }

    public static Task createTask(String className, Map<String, String> params) {
        try {
            Class<?> aClass = Class.forName(className);
            Object o = aClass.newInstance();
            if (TaskFactory.class.isAssignableFrom(aClass)) {
                TaskFactory taskFactory = TaskFactory.class.cast(o);
                return taskFactory.newTask(params);
            } else {
                return Task.class.cast(o);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
