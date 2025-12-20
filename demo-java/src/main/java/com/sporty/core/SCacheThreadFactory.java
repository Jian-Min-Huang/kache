package com.sporty.core;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SCacheThreadFactory implements ThreadFactory {
    private final String namePrefix;

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    SCacheThreadFactory(final String cacheName) {
        this.namePrefix = "scache-" + cacheName + "-worker-";
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread t = new Thread(runnable, namePrefix + threadNumber.getAndIncrement());
        t.setDaemon(false);
        return t;
    }
}
