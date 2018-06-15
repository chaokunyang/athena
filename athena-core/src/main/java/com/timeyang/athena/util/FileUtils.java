package com.timeyang.athena.util;

import com.timeyang.athena.AthenaException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

/**
 * @author https://github.com/chaokunyang
 */
public class FileUtils {

    public static Properties loadPropertiesFile(String filename) {
        try {
            InputStream stream = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(filename);
            if (stream == null) {
                File file = new File(filename);
                if (file.exists()) {
                    stream = new FileInputStream(filename);
                } else {
                    stream = new FileInputStream("conf/" + filename);
                }
            }

            Properties properties = new Properties();
            properties.load(stream);

            stream.close();
            return properties;
        } catch (Exception e) {
            e.printStackTrace();
            String msg = String.format("Can't load file [%s]", filename);
            throw new RuntimeException(msg, e);
        }
    }

    public static String getResourcePath(String resourceName) {
        URL url = Thread.currentThread().getContextClassLoader().getResource(resourceName);
        if(url == null) {
            url = FileUtils.class.getResource(resourceName);
        }
        if (url == null)
            throw new AthenaException(String.format("Resource %s doesn't exists", resourceName));

        return url.getPath();
    }

    public static File getResourceFile(String resourceName) {
        URL url = Thread.currentThread().getContextClassLoader().getResource(resourceName);
        if(url == null) {
            url = FileUtils.class.getResource(resourceName);
        }
        if(url == null)
            throw new AthenaException(String.format("Resource %s doesn't exists", resourceName));
        File file;
        try {
            file = new File(url.toURI());
        } catch (URISyntaxException e) {
            file = new File(url.getPath());
        }
        return file;
    }

}
