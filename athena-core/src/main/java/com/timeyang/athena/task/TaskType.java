package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.TaskExecutor;
import com.timeyang.athena.task.exec.TaskUtils;
import com.timeyang.athena.utill.ClassUtils;
import com.timeyang.athena.utill.SystemUtils;

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
            throw new UnsupportedOperationException("Unsupported task type");
        }
    },
    SPARK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            String params = " --taskId " + task.getTaskId()
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " " + task.getParams();

            String cmd = "spark-submit --master yarn-client --class " + TaskExecutor.class.getCanonicalName() +
                    " --jars " + SystemUtils.CLASSPATH.replaceAll(";", ",")
                    + " " + ClassUtils.findJar(TaskExecutor.class)
                    + " " + params;
            return cmd;
        }
    },
    FLINK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            throw new UnsupportedOperationException("Unsupported task type");
        }
    };

    public abstract String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort);
}
