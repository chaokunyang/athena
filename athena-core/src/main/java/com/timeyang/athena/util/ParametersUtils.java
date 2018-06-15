package com.timeyang.athena.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author https://github.com/chaokunyang
 */
public class ParametersUtils {

    private static final String NO_VALUE = "true";

    private final Map<String, String> data;

    public ParametersUtils(Map<String, String> data) {
        this.data = data;
    }

    public static String toArgs(Map<String, String> args) {
        if (args == null)
            return null;
        return args.entrySet().stream()
                .map(entry -> {
                    if (NO_VALUE.equals(entry.getValue())) {
                        return "-" + entry.getKey();
                    } else {
                        return String.format("--%s %s", entry.getKey(), entry.getValue());
                    }
                }).collect(Collectors.joining(" "));

    }

    public static ParametersUtils fromArgs(String args) {
        return fromArgs(args.split("\\s"));
    }


    public static ParametersUtils fromArgs(String[] args) {
        Map<String, String> map = new HashMap<String, String>(args.length / 2);

        String key = null;
        String value = null;
        boolean expectValue = false;
        for (String arg : args) {
            // check for -- argument
            if (arg.startsWith("--")) {
                key = arg.substring(2);
                map.put(key, NO_VALUE);
                expectValue = true;
            } else if (arg.startsWith("-")) { // check for - argument
                // we are waiting for a value, so this is a - prefixed value (negative number)
                if (expectValue) {
                    if (NumberUtils.isNumber(arg)) { // negative number
                        value = arg;
                        expectValue = false;
                    } else {
                        // We waited for a value but found a new key. So the previous key doesnt have a value.
                        key = arg.substring(1);
                        map.put(key, NO_VALUE);
                        expectValue = true;
                    }
                } else {
                    // we are not waiting for a value, so it's an argument
                    key = arg.substring(1);
                    map.put(key, NO_VALUE);
                    expectValue = true;
                }
            } else {
                if (expectValue) {
                    value = arg;
                    expectValue = false;
                } else {
                    throw new RuntimeException("Error parsing arguments '" + Arrays.toString(args) + "' on '" + arg + "'. Unexpected value. Please prefix values with -- or -.");
                }
            }


            if (value != null) {
                map.put(key, value);
                key = null;
                value = null;
                expectValue = false;
            }
            if (key != null && key.length() == 0) {
                throw new IllegalArgumentException("The input " + Arrays.toString(args) + " contains an empty argument");
            }
        }

        return new ParametersUtils(map);
    }

    public boolean has(String key) {
        return this.data.containsKey(key);
    }

    public Map<String, String> get() {
        return data;
    }

    public String get(String key) {
        return this.data.get(key);
    }

    public String getOrDefault(String key, String defaultValue) {
        return this.data.getOrDefault(key, defaultValue);
    }

    public int getInt(String key) {
        String value = get(key);
        return Integer.valueOf(value);
    }

    public long getLong(String key) {
        String value = get(key);
        return Long.valueOf(value);
    }

    public boolean getBoolean(String key) {
        String value = this.data.getOrDefault(key, "false");
        return Boolean.valueOf(value);
    }

    @Override
    public String toString() {
        return "ParametersUtils{" +
                "data=" + data +
                '}';
    }

}
