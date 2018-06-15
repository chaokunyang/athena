package com.timeyang.athena.utill;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author https://github.com/chaokunyang
 */
public class NetworkUtils {

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Can't get hostname", e);
        }
    }

    public static boolean isHostLocal(String host) {
        return  "localhost".equalsIgnoreCase(host) ||
                "127.0.0.1".equalsIgnoreCase(host) ||
                SystemUtils.HOSTNAME.equalsIgnoreCase(host) ||
                SystemUtils.LOCAL_ADDRESSES.contains(host);
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
