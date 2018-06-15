package com.timeyang.athena.utill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author https://github.com/chaokunyang
 */
public class SystemUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemUtils.class);

    public static String OS_NAME = System.getProperty("os.name").toUpperCase();
    public static boolean IS_LINUX = OS_NAME.startsWith("LINUX");
    public static boolean IS_WINDOWS = OS_NAME.startsWith("WINDOWS");
    public static String WORK_DIR;
    public static String ENCODING;
    public static Set<String> HADOOP_CLASSPATH = ClassUtils.getHadoopClasspath();
    public static Set<String> SPARK_CLASSPATH = ClassUtils.getSparkClasspath();
    public static Set<String> CLASSPATH_SET = ClassUtils.getCurrentClasspath();
    public static String CLASSPATH;
    public static String ATHENA_CLASSPATH;
    public static String HOSTNAME;
    public static Set<String> LOCAL_ADDRESSES;

    static {
        WORK_DIR = getWorkDir();
        LOGGER.info("Work dir: " + WORK_DIR);

        ENCODING = getEncoding();
        LOGGER.info("encoding: " + ENCODING);

        String split = IS_WINDOWS ? ";" : ":";
        CLASSPATH = CLASSPATH_SET.stream().collect(Collectors.joining(split));
        LOGGER.info("classpath: " + CLASSPATH);

        LinkedHashSet<String> athenaClasspath = new LinkedHashSet<>(CLASSPATH_SET);
        athenaClasspath.removeAll(HADOOP_CLASSPATH);
        athenaClasspath.removeAll(SPARK_CLASSPATH);
        ATHENA_CLASSPATH = athenaClasspath.stream().collect(Collectors.joining(split));


        HOSTNAME = NetworkUtils.getHostname();
        LOGGER.info("hostname: " + HOSTNAME);

        LOCAL_ADDRESSES = NetworkUtils.getLocalAddresses();
        LOGGER.info("local addresses: " + LOCAL_ADDRESSES);
    }

    /**
     * Only work in oracle jvm
     *
     * @return java process id
     */
    public static int getPID() {
        String processName =
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(processName.split("@")[0]);
    }

    public static boolean isLinux() {
        return System.getProperty("os.name").toUpperCase().startsWith("LINUX");
    }

    public static String getWorkDir() {
        String workDir = System.getProperty("user.dir");
        return workDir;
    }

    public static String getEncoding() {
        // String encoding = System.getProperty("file.encoding");
        return Charset.defaultCharset().name();
    }

}
