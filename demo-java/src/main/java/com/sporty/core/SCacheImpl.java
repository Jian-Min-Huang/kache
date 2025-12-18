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
import java.util.UUID;
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

    public static final int GC_NETWORK_BUFFER_SECONDS = 10;
    private static final String luaScript =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('del', KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end";

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
        final String serialized = writeJson(sCacheKey, data);
        updateRemoteCache(sCacheKey, serialized);
        invalidateAllLocalCache(sCacheKey);
    }

    @Override
    public Optional<T> getIfPresent(String key) {
        final String sCacheKey = buildSCacheKey(key);

        final T dataFromL1 = readDataFromL1(sCacheKey);
        if (dataFromL1 != null) {
            return Optional.of(dataFromL1);
        }
        final T dataFromL2 = readDataFromL2(sCacheKey);
        if (dataFromL2 != null) {
            return Optional.of(dataFromL2);
        }

        // acquire distributed lock to load data from upstream
        final String lockKey = buildSCacheLockKey(key);
        final String lockValue = UUID.randomUUID().toString();
        boolean locked = false;
        try {
            locked = Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(lockKey, lockValue, upstreamDataLockExpiry.plusSeconds(GC_NETWORK_BUFFER_SECONDS)));
        } catch (Exception e) {
            log.info("Failed to acquire Redis lock for key: {}", lockKey, e);
        }

        if (locked) {
            try {
                final T doubleCheckDataFromL2 = readDataFromL2(sCacheKey);
                if (doubleCheckDataFromL2 != null) {
                    return Optional.of(doubleCheckDataFromL2);
                } else {
                    final T upstreamValue = upstreamDataLoader.apply(key);
                    if (upstreamValue == null) {
                        return Optional.empty();
                    }

                    final String serialized = writeJson(sCacheKey, upstreamValue);
                    updateRemoteCache(sCacheKey, serialized);
                    invalidateAllLocalCacheWithoutException(sCacheKey);

                    return Optional.of(upstreamValue);
                }
            } catch (Exception e) {
                log.error("Failed to load data from upstream for key: {}", sCacheKey, e);
                return Optional.empty();
            } finally {
                try {
                    deleteLockSafely(lockKey, lockValue);
                } catch (Exception e) {
                    log.error("Failed to delete Redis lock for key: {}", lockKey, e);
                }
            }
        } else {
            log.info("Could not acquire lock to load data for key: {}", sCacheKey);
            return Optional.empty();
        }
    }

    private T readDataFromL1(final String sCacheKey) {
        return caffeineCache.getIfPresent(sCacheKey);
    }

    private T readDataFromL2(final String sCacheKey) {
        try {
            final String redisValue = stringRedisTemplate.opsForValue().get(sCacheKey);
            if (redisValue != null) {
                final T fromRedis = objectMapper.readValue(redisValue, clazz);
                sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
                return fromRedis;
            }

            return null;
        } catch (Exception e) {
            log.error("Error occur when read data from L2 and invalidate all L1 for key: {}", sCacheKey, e);

            return null;
        }
    }


    @Override
    public void invalidateAllCache(String key) throws IOException {
        final String sCacheKey = buildSCacheKey(key);
        invalidateRemoteCache(sCacheKey);
        invalidateAllLocalCache(sCacheKey);
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

    private String writeJson(final String sCacheKey, final T data) throws SCacheSerializeException {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            throw new SCacheSerializeException("Failed to serialize cache payload for key: " + sCacheKey, e);
        }
    }

    private void updateRemoteCache(final String sCacheKey, final String serialized) throws SCacheRemoteCacheOperateException {
        try {
            stringRedisTemplate.opsForValue().set(sCacheKey, serialized, remoteCacheExpiry);
        } catch (Exception e) {
            throw new SCacheRemoteCacheOperateException("Failed to write data to Redis cache for key: " + sCacheKey, e);
        }
    }

    private void invalidateRemoteCache(final String sCacheKey) throws SCacheRemoteCacheOperateException {
        try {
            stringRedisTemplate.delete(sCacheKey);
        } catch (Exception e) {
            throw new SCacheRemoteCacheOperateException("Failed to delete Redis cache for key: " + sCacheKey, e);
        }
    }

    private void invalidateAllLocalCache(final String sCacheKey) throws SCacheLocalCacheOperateException {
        try {
            sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
        } catch (Exception e) {
            throw new SCacheLocalCacheOperateException("Failed to invalidate all local cache for key: " + sCacheKey, e);
        }
    }

    private void invalidateAllLocalCacheWithoutException(final String sCacheKey) {
        try {
            sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
        } catch (Exception e) {
            log.error("Failed to invalidate all local cache for key: {}", sCacheKey, e);
        }
    }

    private void deleteLockSafely(final String lockKey, final String lockValue) {
        final Long result = stringRedisTemplate.execute(
                new DefaultRedisScript<>(luaScript, Long.class),
                Collections.singletonList(lockKey),
                lockValue
        );

        if (result == 0) {
            log.error("CRITICAL: Lock expired or was taken by another thread for key: {}", lockKey);
        }
    }
}
