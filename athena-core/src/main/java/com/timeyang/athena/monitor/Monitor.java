package com.timeyang.athena.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

/**
 * @author https://github.com/chaokunyang
 */
public class Monitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Monitor.class);
    private final SystemInfo si = new SystemInfo();
    private final HardwareAbstractionLayer hal = si.getHardware();
    private final OperatingSystem os = si.getOperatingSystem();


}
