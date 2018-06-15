package com.timeyang.athena.task.exec;

import com.timeyang.athena.utill.*;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;


/**
 * TaskExecutor inspired by {@code org.apache.spark.executor.CoarseGrainedExecutorBackend}
 * <p>Download jars, set up URLClassLoader ...</p>
 *
 * @author https://github.com/chaokunyang
 */
public class TaskExecutorLauncher {
    private static final String TASK_EXECUTOR_NAME = "com.timeyang.athena.task.exec.TaskExecutor";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    /**
     * get classpath
     */
    private static URL[] getClassPath(String mainClasspath, String classpathFilePath) {
        List<String> mainClasspathList = Arrays.asList(mainClasspath.split("[;,:]"));
        Set<String> classpath = new LinkedHashSet<>(mainClasspathList); // reserve order

        if (classpathFilePath != null &&
                !classpathFilePath.trim().equalsIgnoreCase("")) {
            File classpathFile = new File(classpathFilePath);
            try {
                String extraClasspathStr = IoUtils.readFile(classpathFile, "UTF-8");
                String[] splits = extraClasspathStr.split("[;,:]");
                for (String split : splits) {
                    if (split.endsWith("*")) {
                        Files.list(Paths.get(split.substring(0, split.length() - 1)))
                                .map(p -> p.toAbsolutePath().toString())
                                .forEach(classpath::add);
                    } else {
                        classpath.add(split);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Set<String> hadoopClasspath = ClassUtils.getHadoopClasspath();
        if (hadoopClasspath.isEmpty()) {
            info("Hadoop not installed");
        } else {
            classpath.addAll(hadoopClasspath);
            info("Add hadoop classpath to task classpath");
        }

        Set<String> sparkClasspath = ClassUtils.getSparkClasspath();
        if (sparkClasspath.isEmpty()) {
            info("Spark not installed");
        } else {
            classpath.addAll(sparkClasspath);
            info("Add spark classpath to task classpath");
        }

        URL[] urls = classpath.stream()
                .map(str -> {
                    try {
                        return new File(str).toURI().toURL();
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                })
                .toArray(URL[]::new);
        return urls;
    }

    private static URLClassLoader createClassLoader(URL[] urls) {
        return new ClassLoaderUtils.ChildFirstURLClassLoader(urls, TaskExecutorLauncher.class.getClassLoader());
    }

    public static int getPID() {
        String processName =
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(processName.split("@")[0]);
    }

    private static void info(Object msg) {
        String time = FORMATTER.format(Instant.now());
        System.out.printf("%s [%s] INFO %s - %s \n",
                time, Thread.currentThread().getName(), TaskExecutorLauncher.class.getName(),
                msg.toString());
    }

    public static void main(String[] args) {
        info("args: " + Arrays.asList(args));
        ParametersUtils parametersUtils = ParametersUtils.fromArgs(args);
        long taskId = parametersUtils.getLong("taskId");
        String taskManagerHost = parametersUtils.get("taskManagerHost");
        int taskManagerPort = parametersUtils.getInt("taskManagerPort");
        String taskFilePath = parametersUtils.get("taskFilePath");
        String mainClasspath = System.getProperty("java.class.path");
        String classpathFile = parametersUtils.get("classpathFile");
        URL[] classpath = getClassPath(mainClasspath, classpathFile);

        info("mainClasspath: " + mainClasspath);
        info("classpathFile: " + classpathFile);
        info("classpath: " + Arrays.asList(classpath));

        URLClassLoader urlClassLoader = createClassLoader(classpath);
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        try {
            Class<?> executorClass = urlClassLoader.loadClass(TASK_EXECUTOR_NAME);
            info("executorClass: " + executorClass);
            Constructor<?> constructor =
                    executorClass.getDeclaredConstructor(long.class, String.class, int.class, String.class);
            Object executor = constructor.newInstance(taskId, taskManagerHost, taskManagerPort, taskFilePath);
            info("executor instance: " + executor);

            info("start task executor");
            ReflectionUtils.invokeMethod(executor, "start");
            info("started task executor");

            info("execute task executor");
            ReflectionUtils.invokeMethod(executor, "execute");
            info("executed task executor ");

            info("stop task executor");
            ReflectionUtils.invokeMethod(executor, "stop");
            info("stopped task executor");

            System.exit(0);
        } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | NoSuchMethodException | IllegalAccessException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
