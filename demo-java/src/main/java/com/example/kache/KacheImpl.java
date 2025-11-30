package com.example.kache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class KacheImpl<T> extends Kache<T> {
    private final Class<T> clazz;
    private final Cache<String, T> caffeineCache;
    private final StringRedisTemplate stringRedisTemplate;
    private final Function<String, T> upstreamDataLoader;
    private final KacheSynchronizer kacheSynchronizer;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Duration LOCK_TTL = Duration.ofSeconds(5);

    public KacheImpl(
            final String identifier,
            final Class<T> clazz,
            final Cache<String, T> caffeineCache,
            final StringRedisTemplate stringRedisTemplate,
            final Function<String, T> upstreamDataLoader,
            final KacheSynchronizer kacheSynchronizer) {
        super(identifier);
        this.clazz = clazz;
        this.caffeineCache = caffeineCache;
        this.stringRedisTemplate = stringRedisTemplate;
        this.upstreamDataLoader = upstreamDataLoader;
        this.kacheSynchronizer = kacheSynchronizer;
    }

    @Override
    public Optional<T> getIfPresent(final String key) {
        final String kacheKey = buildKacheKey(key);

        final T fromCaffeine = caffeineCache.getIfPresent(kacheKey);
        if (fromCaffeine != null) {
            log.debug("Local cache hit for key: {}", kacheKey);
            return Optional.of(fromCaffeine);
        }

        final String lockKey = kacheKey + ":lk";

        try {
            final String redisValue = stringRedisTemplate.opsForValue().get(kacheKey);
            if (redisValue != null) {
                final T fromRedis = objectMapper.readValue(redisValue, clazz);
                caffeineCache.put(kacheKey, fromRedis);
                log.debug("Redis cache hit for key: {}", kacheKey);
                return Optional.of(fromRedis);
            }
        } catch (Exception e) {
            log.error("Failed to read from Redis cache for key: {}", kacheKey, e);
        }

        Boolean locked = Boolean.FALSE;
        try {
            locked = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", LOCK_TTL);
        } catch (Exception e) {
            log.error("Failed to acquire Redis lock for key: {}", lockKey, e);
        }

        if (Boolean.TRUE.equals(locked)) {
            try {
                final T upstreamValue = upstreamDataLoader.apply(key);
                if (upstreamValue == null) {
                    log.debug("Upstream returned null for key: {}", kacheKey);
                    return Optional.empty();
                }

                try {
                    final String serialized = objectMapper.writeValueAsString(upstreamValue);
                    stringRedisTemplate.opsForValue().set(kacheKey, serialized);
                } catch (Exception e) {
                    log.error("Failed to write back to Redis cache for key: {}", kacheKey, e);
                }

                caffeineCache.put(kacheKey, upstreamValue);
                log.debug("Loaded data from upstream for key: {}", kacheKey);
                return Optional.of(upstreamValue);
            } catch (Exception e) {
                log.error("Failed to load data from upstream for key: {}", kacheKey, e);
                return Optional.empty();
            } finally {
                try {
                    stringRedisTemplate.delete(lockKey);
                } catch (Exception e) {
                    log.error("Failed to delete Redis lock for key: {}", lockKey, e);
                }
            }
        } else {
            log.warn("Could not acquire lock to load data for key: {}", kacheKey);
            return Optional.empty();
        }
    }

    // TODO: implement compensate when objectMapper writeValueAsString or redis set
    @Override
    public void put(final String key, final T data) throws IOException {
        final String kacheKey = buildKacheKey(key);

        final String serialized;
        try {
            serialized = objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            log.error("Failed to serialize data for key: {}", kacheKey, e);
            throw new IOException("Failed to serialize cache payload for key: " + kacheKey, e);
        }

        try {
            stringRedisTemplate.opsForValue().set(kacheKey, serialized);
        } catch (Exception e) {
            log.error("Failed to write data to Redis cache for key: {}", kacheKey, e);
            throw new IOException("Failed to write data to Redis cache for key: " + kacheKey, e);
        }

        kacheSynchronizer.publishCacheInvalidation(kacheKey);
    }

    @Override
    public void invalidateLocalCache(final String kacheKey) {
        caffeineCache.invalidate(kacheKey);
        log.debug("Invalidated local cache for key: {}", kacheKey);
    }

    @Override
    public void invalidateAllCache(final String key) throws IOException {
        log.debug("Invalidating all caches for key: {}", key);
        final String kacheKey = buildKacheKey(key);
        try {
            stringRedisTemplate.delete(kacheKey);
        } catch (Exception e) {
            log.error("Failed to delete Redis cache for key: {}", kacheKey, e);
            throw new IOException("Failed to delete Redis cache for key: " + kacheKey, e);
        }

        kacheSynchronizer.publishCacheInvalidation(kacheKey);
    }

    @Override
    public void refresh(final String key) throws IOException {
        final T upstreamValue = upstreamDataLoader.apply(key);
        if (upstreamValue != null) {
            put(key, upstreamValue);
        }
    }
}
