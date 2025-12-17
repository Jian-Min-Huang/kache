package com.sporty.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sporty.exception.SCacheLocalCacheOperateException;
import com.sporty.exception.SCacheRemoteCacheOperateException;
import com.sporty.exception.SCacheSerializeException;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@Log4j2
public class SCacheImpl<T> extends SCache<T> {
    private final Class<T> clazz;
    private final Cache<String, T> caffeineCache;
    private final Duration remoteCacheExpiry;
    private final StringRedisTemplate stringRedisTemplate;
    private final Duration upstreamDataLockExpiry;
    private final Function<String, T> upstreamDataLoader;
    private final SCacheSynchronizer sCacheSynchronizer;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String LOCK_SUFFIX = ":lk";
    private static final String LOCK_VALUE = "1";

    public SCacheImpl(
            final Class<T> clazz,
            final Duration localCacheExpiry,
            final Long maximumSize,
            final Duration remoteCacheExpiry,
            final StringRedisTemplate stringRedisTemplate,
            final Duration upstreamDataLockExpiry,
            final Function<String, T> upstreamDataLoader,
            final SCacheSynchronizer sCacheSynchronizer
    ) {
        super(clazz.getSimpleName());

        this.clazz = clazz;
        this.caffeineCache = Caffeine
                .newBuilder()
                .expireAfterWrite(localCacheExpiry)
                .maximumSize(maximumSize)
                .recordStats()
                .build();
        this.remoteCacheExpiry = remoteCacheExpiry;
        this.stringRedisTemplate = stringRedisTemplate;
        this.upstreamDataLockExpiry = upstreamDataLockExpiry;
        this.upstreamDataLoader = upstreamDataLoader;
        this.sCacheSynchronizer = sCacheSynchronizer;

        sCacheSynchronizer.registerSCache(clazz.getTypeName(), this);
    }

    @Override
    public void put(String key, T data) throws IOException {
        final String sCacheKey = buildSCacheKey(key);

        final String serialized;
        try {
            serialized = objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            log.error("Failed to serialize data for key: {}", sCacheKey, e);
            throw new SCacheSerializeException("Failed to serialize cache payload for key: " + sCacheKey, e);
        }

        try {
            stringRedisTemplate.opsForValue().set(sCacheKey, serialized, remoteCacheExpiry);
        } catch (Exception e) {
            log.error("Failed to write data to Redis cache for key: {}", sCacheKey, e);
            throw new SCacheRemoteCacheOperateException("Failed to write data to Redis cache for key: " + sCacheKey, e);
        }

        try {
            sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
        } catch (Exception e) {
            log.error("Failed to invalidate all local cache for key: {}", sCacheKey, e);
            throw new SCacheLocalCacheOperateException("Failed to invalidate all local cache for key: " + sCacheKey, e);
        }
    }

    @Override
    public Optional<T> getIfPresent(String key) {
        final String sCacheKey = buildSCacheKey(key);

        final T fromCaffeine = caffeineCache.getIfPresent(sCacheKey);
        if (fromCaffeine != null) {
            return Optional.of(fromCaffeine);
        }

        try {
            final String redisValue = stringRedisTemplate.opsForValue().get(sCacheKey);
            if (redisValue != null) {
                final T fromRedis = objectMapper.readValue(redisValue, clazz);
                caffeineCache.put(sCacheKey, fromRedis);
                return Optional.of(fromRedis);
            }
        } catch (Exception e) {
            log.error("Failed to read from Redis cache for key: {}", sCacheKey, e);
        }

        final String lockKey = sCacheKey + LOCK_SUFFIX;
        boolean locked = false;
        try {
            locked = Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(lockKey, LOCK_VALUE, upstreamDataLockExpiry.plusSeconds(10)));
        } catch (Exception e) {
            log.error("Failed to acquire Redis lock for key: {}", lockKey, e);
        }

        if (locked) {
            try {
                // Double check Redis cache after acquiring the lock

                final T upstreamValue = upstreamDataLoader.apply(key);
                if (upstreamValue == null) {
                    return Optional.empty();
                }

                final String serialized = objectMapper.writeValueAsString(upstreamValue);
                stringRedisTemplate.opsForValue().set(sCacheKey, serialized, remoteCacheExpiry);
                try {
                    sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
                } catch (Exception e) {
                    log.error("Failed to invalidate all local cache for key: {}", sCacheKey, e);
                }

                return Optional.of(upstreamValue);
            } catch (Exception e) {
                log.error("Failed to load data from upstream for key: {}", sCacheKey, e);
                return Optional.empty();
            } finally {
                try {
                    stringRedisTemplate.delete(lockKey);
                } catch (Exception e) {
                    log.error("Failed to delete Redis lock for key: {}", lockKey, e);
                }
            }
        } else {
            log.trace("Could not acquire lock to load data for key: {}", sCacheKey);
            return Optional.empty();
        }
    }

    @Override
    public void invalidateAllCache(String key) throws IOException {
        final String sCacheKey = buildSCacheKey(key);
        try {
            stringRedisTemplate.delete(sCacheKey);
        } catch (Exception e) {
            log.error("Failed to delete Redis cache for key: {}", sCacheKey, e);
            throw new SCacheRemoteCacheOperateException("Failed to delete Redis cache for key: " + sCacheKey, e);
        }

        try {
            sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
        } catch (Exception e) {
            log.error("Failed to invalidate all local cache for key: {}", sCacheKey, e);
            throw new SCacheLocalCacheOperateException("Failed to invalidate all local cache for key: " + sCacheKey, e);
        }
    }

    @Override
    public void invalidateLocalCache(String sCacheKey) {
        caffeineCache.invalidate(sCacheKey);
    }

    @Override
    public Set<String> localCacheKeys() {
        return caffeineCache.asMap().keySet();
    }

    @Override
    public void refresh(String key) throws IOException {
        final T upstreamValue = upstreamDataLoader.apply(key);
        if (upstreamValue != null) {
            put(key, upstreamValue);
        }
    }

    private void deleteLockSafely(String lockKey, String expectedValue, long elapsed) {
        String luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "    return redis.call('del', KEYS[1]) " +
                "else " +
                "    return 0 " +
                "end";

        Long result = stringRedisTemplate.execute(
                new DefaultRedisScript<>(luaScript, Long.class),
                Collections.singletonList(lockKey),
                expectedValue
        );

        if (result == 0) {
            log.error("CRITICAL: Lock expired or was taken by another thread for key: {} after {} ms", lockKey, elapsed);
        }
    }

}
