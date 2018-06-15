package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.TaskExecutor;
import com.timeyang.athena.task.exec.TaskUtils;
import com.timeyang.athena.util.ClassUtils;
import com.timeyang.athena.util.FileUtils;
import com.timeyang.athena.util.SystemUtils;

import java.util.stream.Collectors;

/**
 * Task type
 *
 * @author https://github.com/chaokunyang
 */
public enum TaskType {
    BASH {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            throw new UnsupportedOperationException("Unsupported task type");
        }
    },
    JAVA {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            String initCmd = TaskUtils.getTaskInitCmd(task.getTaskId());
            String execCmd = TaskUtils.getTaskExecCmd(task, taskRpcHost, taskRpcPort);

            // &&: execute each command only if the previous one succeeded
            // && behave consistent in windows and linux
            return initCmd + " && " + execCmd;
        }
    },
    MAPREDUCE {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            Long taskId = task.getTaskId();
            String initCmd = TaskUtils.getTaskInitCmd(task.getTaskId());
            String taskLogFilePath = TaskUtils.getTaskLogFilePath(taskId);
            String params = " --taskId " + taskId
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " --taskFilePath " + taskLogFilePath
                    + " " + task.getParams();

            String mainJar = ClassUtils.findJar(TaskExecutor.class);
            String jars = SystemUtils.ATHENA_JARS.stream()
                    .filter(jar -> !jar.equals(mainJar))
                    .collect(Collectors.joining(","));

            String athenaFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String athenaDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" -files %s,%s ", athenaFilePath, athenaDefaultFilePath);

            String redirectOut = " >" + taskLogFilePath;
            String env = "export HADOOP_CLASSPATH=" + SystemUtils.CLASSPATH;
            return initCmd + " && (nohup " + env
                    + " && hadoop jar " + ClassUtils.findJar(TaskExecutor.class)
                    + " " + mainJar
                    + " -libjars " + jars
                    + files
                    + " " + params
                    + redirectOut
                    + " 2>&1 &)";
        }
    },
    SPARK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            Long taskId = task.getTaskId();
            String initCmd = TaskUtils.getTaskInitCmd(task.getTaskId());
            String taskLogFilePath = TaskUtils.getTaskLogFilePath(taskId);
            String params = " --taskId " + task.getTaskId()
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " " + task.getParams();

            String mainJar = ClassUtils.findJar(TaskExecutor.class);
            String jars = SystemUtils.ATHENA_JARS.stream()
                    .filter(jar -> !jar.equals(mainJar))
                    .collect(Collectors.joining(","));

            String athenaFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String athenaDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" -files %s,%s ", athenaFilePath, athenaDefaultFilePath);

            String redirectOut = " >" + taskLogFilePath;
            return initCmd + " && (nohup spark-submit --master yarn-cluster  " +
                    " --conf spark.yarn.submit.waitAppCompletion=false " +
                    " --class " + TaskExecutor.class.getName() +
                    " --jars " + jars
                    + files
                    + " " + mainJar
                    + " " + params
                    + redirectOut
                    + " 2>&1 &)";
        }
    },
    FLINK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            Long taskId = task.getTaskId();
            String initCmd = TaskUtils.getTaskInitCmd(task.getTaskId());

            String taskLogFilePath = TaskUtils.getTaskLogFilePath(taskId);
            String params = " --taskId " + task.getTaskId()
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " " + task.getParams();

            String mainJar = ClassUtils.findJar(TaskExecutor.class);
            String jars = SystemUtils.ATHENA_JARS.stream()
                    .filter(jar -> !jar.equals(mainJar))
                    .collect(Collectors.joining(","));

            String athenaFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String athenaDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" -yt %s,%s ", athenaFilePath, athenaDefaultFilePath);

            String redirectOut = " >" + taskLogFilePath;
            return initCmd + " && (nohup flink run -m yarn-cluster -yn 3 -yjm 4096 -ytm 4096 -ys 8 -yd " +
                    " -c " + TaskExecutor.class.getName() +
                    " -yj " + jars
                    + files
                    + " " + mainJar
                    + " " + params
                    + redirectOut
                    + " 2>&1 &)";
        }
    };

    public abstract String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort);
}
