package com.example.kache;

public interface KacheSynchronizer {
    void invalidateAllLocalCache(final String kacheKey);

    void invalidateLocalCache(final String kacheKey);

    <T> void registerKache(final String identifier, final Kache<T> kache);
}
