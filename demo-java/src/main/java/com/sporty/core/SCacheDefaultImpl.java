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
import java.util.concurrent.*;
import java.util.function.Function;

@Log4j2
public class SCacheDefaultImpl<T> extends SCache<T> {
    private final Class<T> clazz;
    private final Cache<String, T> caffeineCache;
    private final Duration remoteCacheExpiry;
    private final StringRedisTemplate stringRedisTemplate;
    private final Duration upstreamDataLoadTimeout;
    private final Long upstreamDataLoadTimeoutWarningThreshold;
    private final Duration upstreamLockTimeout;
    private final Function<String, T> upstreamDataLoader;
    private final SCacheSynchronizer sCacheSynchronizer;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Executor upstreamExecutor = new ThreadPoolExecutor(8, 16, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16));

    public static final int GC_NETWORK_BUFFER_SECONDS = 10;
    private static final String luaScript =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                    "    return redis.call('del', KEYS[1]) " +
                    "else " +
                    "    return 0 " +
                    "end";

    // 這個 SCacheDefaultImpl 有設計上的取捨與限制：
    // 我們以保護上游服務為優先，因此在無法取得鎖的情況下，會直接回傳空值，已避免對上游服務造成過大壓力，所以用戶端需處理空值的邏輯
    // 與此同時，我們也接受一定時間內的資料不一致性，所以在 put 與 invalidateAllCache 時，用戶端也需要自行處理不同Exception的錯誤情況
    // 目前實作最大的弱點就是上游服務的資料載入時間超過預先規劃的 upstreamDataLoadTimeout
    // 雖然我們會取消該載入請求，但如果上游服務本身沒有實作超時邏輯，仍然可能導致 upstreamExecutor 的線程被長時間佔用，最終導致線程池耗盡
    public SCacheDefaultImpl(
            final Class<T> clazz,
            final Duration localCacheExpiry,
            final Long maximumSize,
            final Duration remoteCacheExpiry,
            final StringRedisTemplate stringRedisTemplate,
            final Duration upstreamDataLoadTimeout,
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
        this.upstreamDataLoadTimeout = upstreamDataLoadTimeout;
        this.upstreamDataLoadTimeoutWarningThreshold = (long) (upstreamDataLoadTimeout.toMillis() * 0.8);
        this.upstreamLockTimeout = upstreamDataLoadTimeout.multipliedBy(2L).plusSeconds(GC_NETWORK_BUFFER_SECONDS);
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

        final String lockKey = buildSCacheLockKey(key);
        final String lockValue = UUID.randomUUID().toString();
        final long startTime = System.currentTimeMillis();
        boolean tryLockResult = tryLock(lockKey, lockValue);
        if (tryLockResult) {
            try {
                final T doubleCheckDataFromL2 = readDataFromL2(sCacheKey);
                if (doubleCheckDataFromL2 != null) {
                    log.warn("Cache hit after acquiring lock for key: {}", sCacheKey);
                    return Optional.of(doubleCheckDataFromL2);
                } else {
                    final T upstreamValue = readDataFromUpstream(key);
                    if (upstreamValue == null) {
                        return Optional.empty();
                    }

                    final String serialized = writeJson(sCacheKey, upstreamValue);
                    updateRemoteCache(sCacheKey, serialized);
                    tryInvalidateAllLocalCache(sCacheKey);

                    return Optional.of(upstreamValue);
                }
            } catch (Exception e) {
                log.error("Failed to load data from upstream for key: {}", sCacheKey, e);
                return Optional.empty();
            } finally {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > upstreamDataLoadTimeoutWarningThreshold) {
                    log.warn("CRITICAL: Lock operation took {} ms, approaching UPSTREAM_DATA_LOAD_TIMEOUT {} ms for key: {}", elapsed, upstreamDataLoadTimeout.toMillis(), sCacheKey);
                }

                deleteLockSafely(lockKey, lockValue);
            }
        } else {
            log.info("Could not acquire lock to load data for key: {}", sCacheKey);
            return Optional.empty();
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

    private T readDataFromL1(final String sCacheKey) {
        return caffeineCache.getIfPresent(sCacheKey);
    }

    private T readDataFromL2(final String sCacheKey) {
        try {
            final String redisValue = stringRedisTemplate.opsForValue().get(sCacheKey);
            if (redisValue != null) {
                final T fromRedis = objectMapper.readValue(redisValue, clazz);
                caffeineCache.put(sCacheKey, fromRedis);
                return fromRedis;
            }

            return null;
        } catch (Exception e) {
            log.error("Error occur when read data from L2 and invalidate all L1 for key: {}", sCacheKey, e);

            return null;
        }
    }

    private T readDataFromUpstream(final String key) {
        final CompletableFuture<T> upstreamDataFuture = CompletableFuture.supplyAsync(() -> upstreamDataLoader.apply(key), upstreamExecutor);
        try {
            return upstreamDataFuture.get(upstreamDataLoadTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Timeout loading data from upstream for key: {}, attempting to cancel", key);
            upstreamDataFuture.cancel(true);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while loading data from upstream for key: {}", key);
            upstreamDataFuture.cancel(true);
            return null;
        } catch (ExecutionException e) {
            log.error("Failed to load data from upstream for key: {}", key, e.getCause());
            return null;
        }
    }

    private boolean tryLock(final String lockKey, final String lockValue) {
        try {
            return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(lockKey, lockValue, upstreamLockTimeout));
        } catch (Exception e) {
            log.info("Failed to acquire Redis lock for key: {}", lockKey, e);

            return false;
        }
    }

    private void tryInvalidateAllLocalCache(final String sCacheKey) {
        try {
            sCacheSynchronizer.invalidateAllLocalCache(sCacheKey);
        } catch (Exception e) {
            log.error("Failed to invalidate all local cache for key: {}", sCacheKey, e);
        }
    }

    private void deleteLockSafely(final String lockKey, final String lockValue) {
        try {
            final Long result = stringRedisTemplate.execute(
                    new DefaultRedisScript<>(luaScript, Long.class),
                    Collections.singletonList(lockKey),
                    lockValue
            );

            if (result == 0) {
                log.error("CRITICAL: Lock expired or was taken by another thread for key: {}", lockKey);
            }
        } catch (Exception e) {
            log.error("Failed to delete Redis lock for key: {}", lockKey, e);
        }
    }
}
