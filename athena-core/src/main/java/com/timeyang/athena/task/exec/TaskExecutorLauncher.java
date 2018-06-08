package com.timeyang.athena.task.exec;

import com.timeyang.athena.utill.ClassLoaderUtils;
import com.timeyang.athena.utill.IoUtils;
import com.timeyang.athena.utill.ParametersUtils;
import com.timeyang.athena.utill.ReflectionUtils;

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

        List<String> hadoopClasspath = getHadoopClasspath();
        if (hadoopClasspath.isEmpty()) {
            info("Hadoop not installed");
        } else {
            classpath.addAll(hadoopClasspath);
            info("Add hadoop classpath to task classpath");
        }

        List<String> sparkClasspath = getSparkClasspath();
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

    private static List<String> getHadoopClasspath() {
        try {
            String[] commands;
            String cmd = "hadoop classpath";
            if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
                commands = new String[]{"cmd", "/c", cmd};
            } else {
                commands = new String[]{"bash", "-c", cmd};
            }
            Process process = Runtime.getRuntime().exec(commands);
            process.waitFor();
            final int bufferSize = 1024;
            final char[] buffer = new char[bufferSize];
            final StringBuilder sb = new StringBuilder();
            Reader reader = null;
            try {
                reader = new InputStreamReader(process.getInputStream(), Charset.defaultCharset().name());
                for (; ; ) {
                    int rsz = 0;
                    try {
                        rsz = reader.read(buffer, 0, buffer.length);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (rsz < 0)
                        break;
                    sb.append(buffer, 0, rsz);
                }

                List<String> hcp = new ArrayList<>();
                String[] splits = sb.toString().split("[:,;]");
                for (String split : splits) {
                    if (split.endsWith("*")) {
                        Files.list(Paths.get(split.substring(0, split.length() - 1)))
                                .map(p -> p.toAbsolutePath().toString())
                                .forEach(hcp::add);
                    } else {
                        hcp.add(split);
                    }
                }
                return hcp;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            // e.printStackTrace();
            return Collections.emptyList();
        }
    }

    private static List<String> getSparkClasspath() {
        String sparkHome = System.getenv("SPARK_HOME");
        try {
            return Files.list(Paths.get(sparkHome, "lib"))
                    .map(p -> p.toAbsolutePath().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
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
