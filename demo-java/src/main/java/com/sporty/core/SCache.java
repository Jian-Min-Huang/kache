package com.sporty.core;

import com.sporty.exception.SCacheBlankKeyException;
import com.sporty.exception.SCacheLocalCacheOperateException;
import com.sporty.exception.SCacheRemoteCacheOperateException;
import com.sporty.exception.SCacheSerializeException;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * @see com.sporty.example.AppConfig
 */
public abstract class SCache<T> {
    protected final String identifier;

    private static final String SCACHE_KEY_PREFIX = "SCACHE";

    protected SCache(final String identifier) {
        this.identifier = identifier;
    }

    protected String buildCacheKey(final String key) throws SCacheBlankKeyException {
        if (key == null || key.isBlank()) {
            throw new SCacheBlankKeyException("The cache key cannot be null or blank.");
        }
        return "%s:%s:%s".formatted(SCACHE_KEY_PREFIX, identifier, key);
    }

    protected String buildCacheLockKey(final String key) {
        return "%s:%s:lock:%s".formatted(SCACHE_KEY_PREFIX, identifier, key);
    }

    /**
     * This method is usually called after data changes to ensure consistency between the Cache and the database.
     *
     * @see com.sporty.example.MemberController
     */
    public abstract void put(final String key, final T data) throws SCacheSerializeException, SCacheRemoteCacheOperateException, SCacheLocalCacheOperateException;

    /**
     * @see com.sporty.example.MemberController
     */
    public abstract Optional<T> getIfPresent(final String key);

    /**
     * This method is usually called after data deletion to ensure consistency between the Cache and the database.
     *
     * @see com.sporty.example.MemberController
     */
    public abstract void invalidateAllCache(final String key) throws SCacheRemoteCacheOperateException, SCacheLocalCacheOperateException;

    /**
     * Received notification then clear local cache.
     */
    abstract void invalidateLocalCache(final String sCacheKey);

    /**
     * Acquire all local cache keys.
     *
     * @see com.sporty.example.MemberScheduler
     */
    public abstract Set<String> localCacheKeys();

    /**
     * @see com.sporty.example.MemberScheduler
     */
    public abstract void refresh(final String key) throws IOException;
}
