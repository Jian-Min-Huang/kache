package com.sporty.kache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class KacheSynchronizer {
    protected final Map<String, Kache<?>> registeredKaches = new ConcurrentHashMap<>();

    <T> void registerKache(final String identifier, final Kache<T> kache) {
        registeredKaches.put(identifier, kache);
    }

    abstract void invalidateAllLocalCache(final String kacheKey);

    protected abstract void invalidateLocalCache(final String kacheKey);
}
