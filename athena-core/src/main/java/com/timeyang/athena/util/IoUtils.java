package com.timeyang.athena.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author https://github.com/chaokunyang
 */
public class IoUtils {

    public static String readFile(File file, String charsetName) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(file.toURI()));
        return new String(encoded, charsetName);
    }

    public static void writeFile(String data, File file) throws IOException {
        Files.write(file.toPath(), data.getBytes());
    }

    public static void writeFile(String data, String path) throws IOException {
        Files.write(Paths.get(path), data.getBytes());
    }

    public static String toString(InputStream in, String encoding) {
        final int bufferSize = 1024;
        final char[] buffer = new char[bufferSize];
        final StringBuilder sb = new StringBuilder();
        Reader reader = null;
        try {
            reader = new InputStreamReader(in, encoding);
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
            return sb.toString();
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
    }

}
