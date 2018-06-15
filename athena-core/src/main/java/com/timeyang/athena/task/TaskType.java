package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.TaskExecutor;
import com.timeyang.athena.task.exec.TaskUtils;
import com.timeyang.athena.util.ClassUtils;
import com.timeyang.athena.util.FileUtils;
import com.timeyang.athena.util.SystemUtils;

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
            String params = " --taskId " + taskId
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " --taskFilePath " + TaskUtils.getTaskLogFilePath(taskId)
                    + " " + task.getParams();
            String athenaFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String athenaDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" -files %s,%s ", athenaFilePath, athenaDefaultFilePath);

            String redirectOut = " >" + TaskUtils.getTaskLogFilePath(taskId);
            String env = "export HADOOP_CLASSPATH=" + SystemUtils.CLASSPATH;
            return "(nohup " + env
                    + " && hadoop jar " + ClassUtils.findJar(TaskExecutor.class)
                    + " " + TaskExecutor.class.getName()
                    + " -libjars " + SystemUtils.ATHENA_CLASSPATH.replaceAll("[;:]", ",")
                    + files
                    + " " + params
                    + redirectOut
                    + " 2>&1 &)";
        }
    },
    SPARK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            String params = " --taskId " + task.getTaskId()
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " " + task.getParams();

            String athenaFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String athenaDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" --files %s,%s ", athenaFilePath, athenaDefaultFilePath);
            return "(nohup spark-submit --master yarn-cluster  " +
                    " --conf spark.yarn.submit.waitAppCompletion=false " +
                    " --class " + TaskExecutor.class.getName() +
                    " --jars " + SystemUtils.ATHENA_CLASSPATH.replaceAll("[;:]", ",")
                    + files
                    + " " + ClassUtils.findJar(TaskExecutor.class)
                    + " " + params
                    + " >/dev/null 2>&1 &)";
        }
    },
    FLINK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            String params = " --taskId " + task.getTaskId()
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " " + task.getParams();

            String athenaFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String athenaDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" -yt %s,%s ", athenaFilePath, athenaDefaultFilePath);

            return "(nohup flink run -m yarn-cluster -yn 3 -yjm 4096 -ytm 4096 -ys 8 -yd " +
                    " -c " + TaskExecutor.class.getName() +
                    " -yj " + SystemUtils.ATHENA_CLASSPATH.replaceAll("[;:]", ",")
                    + files
                    + " " + ClassUtils.findJar(TaskExecutor.class)
                    + " " + params
                    + " >/dev/null 2>&1 &)";
        }
    };

    public abstract String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort);
}
