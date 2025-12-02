package com.sporty.kache;

public interface KacheSynchronizer {
    <T> void registerKache(final String identifier, final Kache<T> kache);

    void invalidateAllLocalCache(final String kacheKey);

    void invalidateLocalCache(final String kacheKey);
}
