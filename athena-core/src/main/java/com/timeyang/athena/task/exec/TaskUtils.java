package com.timeyang.athena.task.exec;

import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.task.TaskInfo;
import com.timeyang.athena.util.ParametersUtils;
import com.timeyang.athena.util.StringUtils;
import com.timeyang.athena.util.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * @author https://github.com/chaokunyang
 */
public class TaskUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskUtils.class);
    private static final AthenaConf athenaConf = AthenaConf.getConf();
    private static final String DEFAULT_TASK_EXEC_DIR_NAME = ".tasks";
    private static final String DEFAULT_TASKS_DIR_PATH = getDefaultTasksDir();
    private static final String TASK_LOG_FILE_NAME = "task.log";
    private static final String CLASSPATH_SPLIT;

    static {
        if (SystemUtils.IS_WINDOWS) {
            CLASSPATH_SPLIT = ";";
        } else {
            CLASSPATH_SPLIT = ":";
        }
    }

    private static String getDefaultTasksDir() {
        return Paths.get(SystemUtils.WORK_DIR, DEFAULT_TASK_EXEC_DIR_NAME).toAbsolutePath().toString();
    }

    public static String getTasksDir() {
        String tasksDir = athenaConf.get("task.exec.tasks.dir");

        if (tasksDir == null) {
            tasksDir = DEFAULT_TASKS_DIR_PATH;
        }

        return Paths.get(tasksDir).toAbsolutePath().toString();
    }

    public static String getExecTaskDir(long taskId) {
        String tasksDir = getTasksDir();
        return Paths.get(tasksDir, String.valueOf(taskId)).toAbsolutePath().toString();
    }

    public static String getTaskInitCmd(long taskId) {
        String taskDir = getExecTaskDir(taskId);
        if (SystemUtils.IS_WINDOWS) {
            // must add brackets to "if not exist", or else following commands will bt treated as on command. No need for linux.
            return String.format("(if not exist %s mkdir %s)", taskDir, taskDir);
        } else {
            // add '' to cmd to compose a cmd
            return String.format("test -d %s || mkdir -p %s", taskDir, taskDir);
        }
    }

    public static String getTaskExecCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
        Long taskId = task.getTaskId();

        String params = task.getParams();
        if (!StringUtils.hasText(params)) {
            params = "";
        }

        String classpath = buildClasspath(task);

        if (StringUtils.hasText(classpath)) {
            classpath = " -classpath \"" + classpath + "\" ";
        } else {
            classpath = " ";
        }

        params = " --taskId " + taskId
                + " --taskManagerHost " + taskRpcHost
                + " --taskManagerPort " + taskRpcPort
                + " --taskFilePath " + getTaskLogFilePath(taskId)
                + " " + params;
        String redirectOut = " >" + getTaskLogFilePath(taskId)
                + " 2>&1 &";
        String cmd = "java -server -XX:OnOutOfMemoryError=kill "
                + classpath
                + TaskExecutorLauncher.class.getName()
                + params + " "
                + redirectOut;
        if (SystemUtils.IS_WINDOWS) {
            cmd = cmd.replaceAll("/", "\\\\");
        }
        if (SystemUtils.isLinux()) {
            // & has higher precedence than &&. use sub shell to workaround this
            // see https://stackoverflow.com/questions/15934751/nohup-doesnt-work-when-used-with-double-ampersand-instead-of-semicolon
            cmd = "(nohup " + cmd + ")";
        }

        return cmd;
    }

    private static String buildClasspath(TaskInfo task) {
        String params = task.getParams();
        if (!StringUtils.hasText(params)) {
            params = "";
        }

        StringBuilder classpathBuilder = new StringBuilder();
        String athenaClasspath = getAthenaClasspath();
        if (StringUtils.hasText(athenaClasspath)) {
            classpathBuilder.append(athenaClasspath);
        }
        if (StringUtils.hasText(task.getLibs())) {
            if (StringUtils.hasText(classpathBuilder)) {
                classpathBuilder.append(CLASSPATH_SPLIT);
            }
            classpathBuilder.append(task.getLibs());
        }

        return classpathBuilder.toString();
    }

    private static String getAthenaClasspath() {
        // try {
        //     return Files.list(Paths.get(".", "lib"))
        //             .map(p -> p.toAbsolutePath().toString())
        //             .collect(Collectors.joining(CLASSPATH_SPLIT));
        // } catch (IOException e) {
        //     return "";
        // }

        if (Files.exists(Paths.get(".", "lib"))) {
            return Paths.get(".", "lib").toAbsolutePath().toString() + "/*";
        } else {
            return "";
        }
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

    public static String getTaskLogFilePath(long taskId) {
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
        } catch (ClassNotFoundException e) {
            LOGGER.error("Can't find task class {}, mark task failed", className, e);
            throw new RuntimeException(e);
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.error("can't instantiate task class, mark task failed", e);
            throw new RuntimeException(e);
        }
    }

}
