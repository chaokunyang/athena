package com.timeyang.athena.task;

import com.timeyang.athena.task.exec.TaskExecutor;
import com.timeyang.athena.task.exec.TaskUtils;
import com.timeyang.athena.util.ClassUtils;
import com.timeyang.athena.util.FileUtils;
import com.timeyang.athena.util.ParametersUtils;
import com.timeyang.athena.util.StringUtils;
import com.timeyang.athena.util.SystemUtils;

import java.util.HashMap;
import java.util.Map;
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

            Map<String, String> args = ParametersUtils.fromArgs(task.getParams()).get();
            String sparkOptions = args.getOrDefault("__spark__", "");
            args.remove("__spark__");

            String taskLogFilePath = TaskUtils.getTaskLogFilePath(taskId);
            String params = " --taskId " + task.getTaskId()
                    + " --taskManagerHost " + taskRpcHost
                    + " --taskManagerPort " + taskRpcPort
                    + " " + ParametersUtils.toArgs(args);

            String pFilePath = FileUtils.getResourceFile("athena.properties").getAbsolutePath();
            String pDefaultFilePath = FileUtils.getResourceFile("athena-default.properties").getAbsolutePath();
            String files = String.format(" --files %s,%s ", pFilePath, pDefaultFilePath);
            String mainJar = ClassUtils.findJar(TaskExecutor.class);
            String jars = SystemUtils.ATHENA_JARS.stream()
                    .filter(jar -> !jar.equals(mainJar))
                    .collect(Collectors.joining(","));
            String redirect = " >" + taskLogFilePath;

            return initCmd + " && (nohup spark-submit " +
                    configureSparkOptions(sparkOptions) +
                    " --class " + TaskExecutor.class.getName() +
                    " --jars " + jars
                    + files
                    + " " + mainJar
                    + " " + params
                    + redirect
                    + " 2>&1 &)";
        }
    },
    PYTHON {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            Long taskId = task.getTaskId();
            String initCmd = TaskUtils.getTaskInitCmd(task.getTaskId());

            String params = task.getParams();
            if (!StringUtils.hasText(params)) {
                params = "";
            }

            params = " --task_id " + task.getTaskId()
                    + " --task_manager_host " + taskRpcHost
                    + " --task_manager_port " + taskRpcPort
                    + " " + params;

            String setPythonPath = " export PYTHONPATH=${ATHENA_HOME}/python/lib/`ls pyathena*` && ";
            if (SystemUtils.IS_WINDOWS) {
                setPythonPath = " "; // In WINDOWS, install pyathena instead
            }

            String redirect = " >" + TaskUtils.getTaskLogFilePath(taskId);
            String execCmd = "python -m athena.task_executor"
                    + " --entry_point " + task.getClassName()
                    + " " + params
                    + redirect
                    + " 2>&1 &";

            if (SystemUtils.isLinux()) {
                execCmd = "(nohup " + execCmd + ")"; // & has higher precedence than &&. use sub shell to workaround this
            }
            return initCmd + " &&  " + setPythonPath
                    + " " + execCmd;
        }
    },
    PYSPARK {
        @Override
        public String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort) {
            Long taskId = task.getTaskId();
            String initCmd = TaskUtils.getTaskInitCmd(task.getTaskId());

            Map<String, String> args = ParametersUtils.fromArgs(task.getParams()).get();
            String sparkOptions = args.getOrDefault("__spark__", "");
            args.remove("__spark__");
            String params = " --task_id " + task.getTaskId()
                    + " --task_manager_host " + taskRpcHost
                    + " --task_manager_port " + taskRpcPort
                    + " " + ParametersUtils.toArgs(args);

            String setPythonPath = " export PYTHONPATH=${ATHENA_HOME}/python/lib/`ls pyathena*` ";
            String mainPy = " ${ATHENA_HOME}/python/athena/task_executor.py ";
            String redirect = " >" + TaskUtils.getTaskLogFilePath(taskId);

            return initCmd + " && " + setPythonPath
                    + " && (nohup spark-submit "
                    + configureSparkOptions(sparkOptions)
                    + " --py-files " + ""
                    + mainPy
                    + " --entry_point" + task.getClassName()
                    + " " + params
                    + redirect
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

    private static String configureSparkOptions(String sparkOptions) {
        String[] splits = sparkOptions.trim().split("\\s+");
        Map<String, String> options = new HashMap<>();
        options.put("--master", "yarn-client");

        Map<String, String> conf = new HashMap<>();
        for (int i = 0; i < splits.length; i += 2) {
            if (splits[i].trim().equals("--conf")) {
                String[] kv = splits[i + 1].trim().split("=");
                conf.put(kv[0], kv[1]);
            } else {
                options.put(splits[i], splits[i + 1]);
            }
        }
        conf.put("spark.yarn.submit.waitAppCompletion", "false");
        conf.put("spark.yarn.maxAppAttempts", "1");
        options.remove("--class");

        StringBuilder sb = new StringBuilder();
        options.forEach((key, value) -> {
            sb.append(' ');
            sb.append(key);
            sb.append(' ');
            sb.append(value);
        });
        conf.forEach((key, value) -> {
            sb.append(" --conf ");
            sb.append(key);
            sb.append('=');
            sb.append(value);
        });
        return sb.toString();
    }

    public abstract String getTaskCmd(TaskInfo task, String taskRpcHost, int taskRpcPort);
}
