package com.timeyang.athena.util;

import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author https://github.com/chaokunyang
 */
public class ThreadUtils {

    public static ThreadFactory createThreadFactory(String groupName) {
        return new ThreadFactory() {
            private AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(groupName + "-" + count.getAndIncrement());
                thread.setUncaughtExceptionHandler((t, e) -> {
                    LoggerFactory.getLogger(t.getName()).error(e.getMessage(), e);
                });
                return thread;
            }
        };
    }

}
