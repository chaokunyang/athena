package com.timeyang.athena.utill;

/**
 * @author https://github.com/chaokunyang
 */
public class NetworkUtils {

    public static boolean isHostLocal(String host) {
        return  "localhost".equalsIgnoreCase(host) ||
                "127.0.0.1".equalsIgnoreCase(host) ||
                SystemUtils.HOSTNAME.equalsIgnoreCase(host) ||
                SystemUtils.LOCAL_ADDRESSES.contains(host);
    }

}
