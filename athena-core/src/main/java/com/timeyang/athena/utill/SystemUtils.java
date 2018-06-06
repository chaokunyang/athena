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
    public static String CLASSPATH;
    public static String HOSTNAME;
    public static Set<String> LOCAL_ADDRESSES;

    static {
        WORK_DIR = getWorkDir();
        LOGGER.info("Work dir: " + WORK_DIR);

        ENCODING = getEncoding();
        LOGGER.info("encoding: " + ENCODING);

        CLASSPATH = getCurrentClasspath();
        LOGGER.info("classpath: " + CLASSPATH);

        HOSTNAME = getHostname();
        LOGGER.info("hostname: " + HOSTNAME);

        LOCAL_ADDRESSES = getLocalAddresses();
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

    public static String getCurrentClasspath() {
        // return System.getProperty("java.class.path");
        String split;
        if (IS_WINDOWS) {
            split = ";";
        } else {
            split = ":";
        }
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        Set<URL> urls = new LinkedHashSet<>(Arrays.asList(((URLClassLoader) cl).getURLs()));

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader instanceof URLClassLoader) {
            urls.addAll(Arrays.asList(((URLClassLoader) contextClassLoader).getURLs()));
        }

        return urls.stream().map(url -> {
            try {
                return new File((url.toURI())).getAbsolutePath();
            } catch (URISyntaxException e) {
                throw new IllegalStateException();
            }
        }).collect(Collectors.joining(split));
    }

    public static String getEncoding() {
        // String encoding = System.getProperty("file.encoding");
        return Charset.defaultCharset().name();
    }

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Can't get hostname", e);
        }
    }

    public static Set<String> getLocalAddresses() {
        Set<String> addresses = new HashSet<>();
        try {
            List<NetworkInterface> networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
            for (NetworkInterface ifc : networkInterfaces) {
                if (ifc.isUp()) {
                    for (InetAddress address : Collections.list(ifc.getInetAddresses())) {
                        addresses.add(address.getHostAddress());
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }

        return addresses;
    }
}
