package com.timeyang.athena.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author https://github.com/chaokunyang
 */
public class ClassUtils {

    public static String findJar(Class<?> clz) {
        URL url = getClassLocation(clz);
        return Objects.requireNonNull(url).getPath();
    }

    public static URL getClassLocation(Class<?> c)
    {
        URL url = c.getResource(c.getSimpleName() + ".class");
        if (url == null)
        {
            return null;
        }
        String s = url.toExternalForm();
        // s most likely ends with a /, then the full class name with . replaced
        // with /, and .class. Cut that part off if present. If not also check
        // for backslashes instead. If that's also not present just return null

        String end = "/" + c.getName().replaceAll("\\.", "/") + ".class";
        if (s.endsWith(end))
        {
            s = s.substring(0, s.length() - end.length());
        }
        else
        {
            end = end.replaceAll("/", "\\");
            if (s.endsWith(end))
            {
                s = s.substring(0, s.length() - end.length());
            }
            else
            {
                return null;
            }
        }
        // s is now the URL of the location, but possibly with jar: in front and
        // a trailing !
        if (s.startsWith("jar:") && s.endsWith("!"))
        {
            s = s.substring(4, s.length() - 1);
        }
        try
        {
            return new URL(s);
        }
        catch (MalformedURLException e)
        {
            return null;
        }
    }

    public static Set<String> getCurrentClasspath() {
        // return System.getProperty("java.class.path");

        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)systemClassLoader).getURLs();
        Set<URL> cp = new LinkedHashSet<>(Arrays.asList(urls));

        ClassLoader classLoader = SystemUtils.class.getClassLoader();
        if (classLoader instanceof  URLClassLoader) {
            URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            cp.addAll(Arrays.asList(urlClassLoader.getURLs()));
        }

        return cp.stream().map(url -> {
            try {
                return new File((url.toURI())).getAbsolutePath();
            } catch (URISyntaxException e) {
                throw new IllegalStateException();
            }
        }).collect(Collectors.toSet());
    }

    public static Set<String> getHadoopClasspath() {
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

                Set<String> hcp = new LinkedHashSet<>();
                String[] splits = sb.toString().split("[:,;]");
                for (String split : splits) {
                    if (split.endsWith("*")) {
                        Files.list(Paths.get(split.substring(0, split.length() - 1)))
                                .filter(path -> Files.exists(path))
                                .map(p -> p.toAbsolutePath().toString())
                                .forEach(hcp::add);
                    } else {
                        if (Files.exists(Paths.get(split))) {
                            hcp.add(split);
                        }
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
            return Collections.emptySet();
        }
    }

    public static Set<String> getSparkClasspath() {
        String sparkHome = System.getenv("SPARK_HOME");
        Path libPath = Paths.get(sparkHome, "lib");
        if (!Files.exists(libPath)) {
            libPath = Paths.get(sparkHome, "jars");
        }
        try {
            return Files.list(libPath)
                    .map(p -> {
                        try {
                            return p.toRealPath().toAbsolutePath().toString();
                        } catch (java.io.IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptySet();
        }
    }

}
