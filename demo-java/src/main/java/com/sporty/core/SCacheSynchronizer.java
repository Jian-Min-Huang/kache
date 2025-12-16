package com.sporty.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class SCacheSynchronizer {
    protected final Map<String, SCache<?>> registeredSCaches = new ConcurrentHashMap<>();

    <T> void registerSCache(final String identifier, final SCache<T> sCache) {
        registeredSCaches.put(identifier, sCache);
    }

    abstract void invalidateAllLocalCache(final String sCacheKey);

    protected abstract void invalidateLocalCache(final String sCacheKey);
}
