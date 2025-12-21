package com.sporty.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.sporty.exception.SCacheLocalCacheOperateException;
import com.sporty.exception.SCacheRemoteCacheOperateException;
import com.sporty.exception.SCacheSerializeException;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Log4j2
public class SCacheDefaultImpl<T> extends SCache<T> implements DisposableBean {
    private final Class<T> clazz;
    private final Cache<String, T> caffeineCache;
    private final Duration remoteCacheExpiry;
    private final StringRedisTemplate stringRedisTemplate;
    private final Duration upstreamDataLoadTimeout;
    private final Long upstreamDataLoadTimeoutWarningThreshold;
    private final Duration upstreamDataLoadLockTimeout;
    private final Function<String, T> upstreamDataLoader;
    private final Executor upstreamDataLoadExecutor;
    private final SCacheSynchronizer sCacheSynchronizer;

    /**
     * Currently using Jackson for serialization/deserialization is a design trade-off; we may consider supporting other serialization frameworks in the future to improve performance.
     */
    private final ObjectMapper objectMapper = new ObjectMapper()
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .registerModule(new JavaTimeModule());

    /**
     * Threshold ratio to log warnings if upstream data load time exceeds this percentage of the timeout.
     * For example, with a ratio of 0.8 and a timeout of 1000ms, a warning will be logged if the load takes longer than 800ms.
     */
    private static final double TIMEOUT_WARNING_THRESHOLD_RATIO = 0.8;

    /**
     * Multiplier to determine the lock timeout duration based on the upstream data load timeout.
     * For example, if the upstream data load timeout is 30 seconds and the multiplier is 2 and gc network buffer is 10 seconds,
     * the lock timeout will be set to 70 seconds (30 * 2 + 10).
     */
    private static final long LOCK_TIMEOUT_MULTIPLIER = 2L;

    /**
     * @see #LOCK_TIMEOUT_MULTIPLIER
     */
    private static final int GC_NETWORK_BUFFER_SECONDS = 10;

    /**
     * Lua script to safely release a Redis lock by checking ownership before deletion.
     */
    private static final String luaScript = """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """;

    /**
     * Timeout in seconds to wait for the upstream data load executor to terminate during shutdown.
     */
    private static final int THREAD_POOL_EXECUTOR_AWAIT_TERMINATION_SECONDS = 30;

    /**
     * Note:
     * <pre>
     * This SCacheDefaultImpl has design trade-offs and limitations:
     *
     * 1. We prioritize protecting upstream services, so if we cannot acquire the lock, we return an empty value to avoid putting too much pressure on upstream services. Therefore, clients need to handle the empty value situation.
     * 2. We also accept data inconsistency within a certain time frame, so when calling <b>put</b> and <b>invalidateAllCache</b> methods, clients need to handle different Exceptions.
     * 3. The biggest weakness of the current implementation is when the upstream service's data loading time exceeds the pre-planned <b>upstreamDataLoadTimeout</b>.
     * 4. Although we have implemented a mechanism to cancel that load, it only works for the client side. If the upstream service itself does not implement a timeout logic, it may still lead to the <b>upstreamExecutor</b> threads being occupied for a long time, potentially exhausting the thread pool and throw RejectedExecutionException.
     * </pre>
     */
    private SCacheDefaultImpl(final Builder<T> builder) {
        super(builder.clazz.getSimpleName());

        this.clazz = builder.clazz;
        this.caffeineCache = Caffeine
                .newBuilder()
                .expireAfterWrite(builder.localCacheExpiry)
                .maximumSize(builder.maximumSize)
                .recordStats()
                .build();
        this.remoteCacheExpiry = builder.remoteCacheExpiry;
        this.stringRedisTemplate = builder.stringRedisTemplate;
        this.upstreamDataLoadTimeout = builder.upstreamDataLoadTimeout;
        this.upstreamDataLoadTimeoutWarningThreshold = (long) (builder.upstreamDataLoadTimeout.toMillis() * TIMEOUT_WARNING_THRESHOLD_RATIO);
        this.upstreamDataLoadLockTimeout = builder.upstreamDataLoadTimeout.multipliedBy(LOCK_TIMEOUT_MULTIPLIER).plusSeconds(GC_NETWORK_BUFFER_SECONDS);
        this.upstreamDataLoader = builder.upstreamDataLoadFunction;
        this.upstreamDataLoadExecutor = new ThreadPoolExecutor(
                builder.upstreamDataLoadPoolSize,
                builder.upstreamDataLoadPoolSize,
                0L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(builder.upstreamDataLoadPoolSize * 2),
                new SCacheThreadFactory(builder.clazz.getTypeName()),
                new ThreadPoolExecutor.AbortPolicy()
        );
        this.sCacheSynchronizer = builder.sCacheSynchronizer;

        sCacheSynchronizer.registerSCache(clazz.getTypeName(), this);
    }

    public static <T> Builder<T> builder(final Class<T> clazz) {
        return new Builder<>(clazz);
    }

    public static class Builder<T> {
        private final Class<T> clazz;
        private Duration localCacheExpiry;
        private Long maximumSize;
        private Duration remoteCacheExpiry;
        private StringRedisTemplate stringRedisTemplate;
        private Duration upstreamDataLoadTimeout;
        private Function<String, T> upstreamDataLoadFunction;
        private Integer upstreamDataLoadPoolSize;
        private SCacheSynchronizer sCacheSynchronizer;

        private Builder(final Class<T> clazz) {
            this.clazz = clazz;
        }

        public Builder<T> localCacheExpiry(final Duration localCacheExpiry) {
            this.localCacheExpiry = localCacheExpiry;
            return this;
        }

        public Builder<T> maximumSize(final Long maximumSize) {
            this.maximumSize = maximumSize;
            return this;
        }

        public Builder<T> remoteCacheExpiry(final Duration remoteCacheExpiry) {
            this.remoteCacheExpiry = remoteCacheExpiry;
            return this;
        }

        public Builder<T> stringRedisTemplate(final StringRedisTemplate stringRedisTemplate) {
            this.stringRedisTemplate = stringRedisTemplate;
            return this;
        }

        public Builder<T> upstreamDataLoadTimeout(final Duration upstreamDataLoadTimeout) {
            this.upstreamDataLoadTimeout = upstreamDataLoadTimeout;
            return this;
        }

        public Builder<T> upstreamDataLoadFunction(final Function<String, T> upstreamDataLoadFunction) {
            this.upstreamDataLoadFunction = upstreamDataLoadFunction;
            return this;
        }

        public Builder<T> upstreamDataLoadPoolSize(final Integer upstreamDataLoadPoolSize) {
            this.upstreamDataLoadPoolSize = upstreamDataLoadPoolSize;
            return this;
        }

        public Builder<T> sCacheSynchronizer(final SCacheSynchronizer sCacheSynchronizer) {
            this.sCacheSynchronizer = sCacheSynchronizer;
            return this;
        }

        public SCacheDefaultImpl<T> build() {
            if (localCacheExpiry == null || localCacheExpiry.isNegative() || localCacheExpiry.isZero()) {
                throw new IllegalArgumentException("localCacheExpiry is null or invalid");
            }
            if (maximumSize == null || maximumSize <= 0) {
                throw new IllegalArgumentException("maximumSize is null or invalid");
            }
            if (remoteCacheExpiry == null || remoteCacheExpiry.isNegative() || remoteCacheExpiry.isZero()) {
                throw new IllegalArgumentException("remoteCacheExpiry is null or invalid");
            }
            if (stringRedisTemplate == null) {
                throw new IllegalArgumentException("stringRedisTemplate is null");
            }
            if (upstreamDataLoadTimeout == null || upstreamDataLoadTimeout.isNegative() || upstreamDataLoadTimeout.isZero()) {
                throw new IllegalArgumentException("upstreamDataLoadTimeout is null or invalid");
            }
            if (upstreamDataLoadFunction == null) {
                throw new IllegalArgumentException("upstreamDataLoadFunction is null");
            }
            if (upstreamDataLoadPoolSize == null || upstreamDataLoadPoolSize <= 0) {
                throw new IllegalArgumentException("upstreamDataLoadPoolSize is null or invalid");
            }
            if (sCacheSynchronizer == null) {
                throw new IllegalArgumentException("sCacheSynchronizer is null");
            }

            return new SCacheDefaultImpl<>(this);
        }
    }

    @Override
    public void put(final String key, final T data) throws SCacheSerializeException, SCacheRemoteCacheOperateException, SCacheLocalCacheOperateException {
        final String cacheKey = buildCacheKey(key);
        final String serialized = writeJson(cacheKey, data);
        updateRemoteCache(cacheKey, serialized);
        invalidateAllLocalCache(cacheKey);
    }

    @Override
    public Optional<T> getIfPresent(final String key) {
        final String cacheKey = buildCacheKey(key);

        final Optional<T> dataFromL1 = readDataFromL1(cacheKey);
        if (dataFromL1.isPresent()) {
            log.debug("L1 cache hit for key: {}", cacheKey);
            return dataFromL1;
        }

        final Optional<T> dataFromL2 = readDataFromL2(cacheKey);
        if (dataFromL2.isPresent()) {
            log.debug("L2 cache hit for key: {}", cacheKey);
            return dataFromL2;
        }

        return readDataFromUpstream(key);
    }

    @Override
    public void invalidateAllCache(final String key) throws SCacheRemoteCacheOperateException, SCacheLocalCacheOperateException {
        final String cacheKey = buildCacheKey(key);
        invalidateRemoteCache(cacheKey);
        invalidateAllLocalCache(cacheKey);
    }

    @Override
    public void invalidateLocalCache(final String cacheKey) {
        caffeineCache.invalidate(cacheKey);
    }

    @Override
    public Set<String> localCacheKeys() {
        return caffeineCache.asMap().keySet();
    }

    @Override
    public void refresh(final String key) throws IOException {
        final T upstreamValue = upstreamDataLoader.apply(key);
        if (upstreamValue != null) {
            put(key, upstreamValue);
        }
    }

    @Override
    public void destroy() {
        log.info("Shutting down upstream executor for cache: {}", clazz.getTypeName());

        if (upstreamDataLoadExecutor instanceof ThreadPoolExecutor tpe) {
            tpe.shutdown();
            try {
                if (!tpe.awaitTermination(THREAD_POOL_EXECUTOR_AWAIT_TERMINATION_SECONDS, TimeUnit.SECONDS)) {
                    List<Runnable> droppedTasks = tpe.shutdownNow();
                    log.warn("Executor did not terminate gracefully, {} tasks dropped", droppedTasks.size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                tpe.shutdownNow();
            }
        }
    }

    private String writeJson(final String cacheKey, final T data) throws SCacheSerializeException {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            throw new SCacheSerializeException("Failed to serialize cache payload for key: " + cacheKey, e);
        }
    }

    private void updateRemoteCache(final String cacheKey, final String serialized) throws SCacheRemoteCacheOperateException {
        try {
            stringRedisTemplate.opsForValue().set(cacheKey, serialized, remoteCacheExpiry);
        } catch (Exception e) {
            throw new SCacheRemoteCacheOperateException("Failed to write data to Redis cache for key: " + cacheKey, e);
        }
    }

    private void invalidateRemoteCache(final String cacheKey) throws SCacheRemoteCacheOperateException {
        try {
            stringRedisTemplate.delete(cacheKey);
        } catch (Exception e) {
            throw new SCacheRemoteCacheOperateException("Failed to delete Redis cache for key: " + cacheKey, e);
        }
    }

    private void invalidateAllLocalCache(final String cacheKey) throws SCacheLocalCacheOperateException {
        try {
            sCacheSynchronizer.invalidateAllLocalCache(cacheKey);
        } catch (Exception e) {
            throw new SCacheLocalCacheOperateException("Failed to invalidate all local cache for key: " + cacheKey, e);
        }
    }

    private void invalidateAllLocalCacheSilently(final String cacheKey) {
        try {
            sCacheSynchronizer.invalidateAllLocalCache(cacheKey);
        } catch (Exception e) {
            log.error("Failed to invalidate local cache across instances for key: {}, may cause temporary inconsistency", cacheKey, e);
        }
    }

    private Optional<T> readDataFromL1(final String cacheKey) {
        return Optional.ofNullable(caffeineCache.getIfPresent(cacheKey));
    }

    private Optional<T> readDataFromL2(final String cacheKey) {
        try {
            final String redisValue = stringRedisTemplate.opsForValue().get(cacheKey);
            if (redisValue != null && !redisValue.isBlank()) {
                final T fromRedis = objectMapper.readValue(redisValue, clazz);
                caffeineCache.put(cacheKey, fromRedis);
                return Optional.of(fromRedis);
            }

            return Optional.empty();
        } catch (Exception e) {
            log.error("Failed to read from L2 cache for key: {}, returning null", cacheKey, e);

            return Optional.empty();
        }
    }

    private Optional<T> readDataFromUpstream(final String key) {
        final String cacheKey = buildCacheKey(key);
        final String cacheLockKey = buildCacheLockKey(key);
        final String cacheLockValue = UUID.randomUUID().toString();
        final long startTime = System.currentTimeMillis();
        boolean tryLockResult = tryLock(cacheLockKey, cacheLockValue);
        if (tryLockResult) {
            try {
                final Optional<T> doubleCheckDataFromL2 = readDataFromL2(cacheKey);
                if (doubleCheckDataFromL2.isPresent()) {
                    log.info("L2 cache hit on double-check after lock acquisition for key: {}", cacheKey);
                    return doubleCheckDataFromL2;
                } else {
                    final T upstreamValue = handleUpstream(key);
                    if (upstreamValue == null) {
                        log.info("Upstream returned null for key: {}", cacheKey);
                        return Optional.empty();
                    }

                    final String serialized = writeJson(cacheKey, upstreamValue);
                    updateRemoteCache(cacheKey, serialized);
                    invalidateAllLocalCacheSilently(cacheKey);
                    log.info("Successfully loaded and cached data from upstream for key: {}", cacheKey);

                    return Optional.of(upstreamValue);
                }
            } catch (Exception e) {
                log.error("Failed to load data from upstream for key: {}, returning empty", cacheKey, e);
                return Optional.empty();
            } finally {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > upstreamDataLoadTimeoutWarningThreshold) {
                    log.error("CRITICAL: Upstream load operation took {} ms (>80% of timeout {} ms) for key: {}", elapsed, upstreamDataLoadTimeout.toMillis(), cacheKey);
                }

                releaseLock(cacheLockKey, cacheLockValue);
            }
        } else {
            log.info("Lock already held by another instance for key: {}, returning empty to protect upstream", cacheKey);
            return Optional.empty();
        }
    }

    private T handleUpstream(final String key) {
        final CompletableFuture<T> upstreamDataFuture = CompletableFuture.supplyAsync(() -> upstreamDataLoader.apply(key), upstreamDataLoadExecutor);
        try {
            return upstreamDataFuture.get(upstreamDataLoadTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.error("Upstream data load timed out after {} ms for key: {}, cancelling task and returning null", upstreamDataLoadTimeout.toMillis(), key);
            upstreamDataFuture.cancel(true);
            return null;
        } catch (RejectedExecutionException e) {
            log.error("Upstream executor pool is full, rejecting task for key: {}", key, e);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while loading data from upstream for key: {}, cancelling task and returning null", key, e);
            upstreamDataFuture.cancel(true);
            return null;
        } catch (ExecutionException e) {
            log.error("Upstream data loader threw exception for key: {}, returning null", key, e);
            return null;
        }
    }

    private boolean tryLock(final String cacheLockKey, final String cacheLockValue) {
        try {
            return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(cacheLockKey, cacheLockValue, upstreamDataLoadLockTimeout));
        } catch (Exception e) {
            log.warn("Exception while attempting to acquire Redis lock for key: {}, treating as lock failure", cacheLockKey, e);

            return false;
        }
    }

    private void releaseLock(final String cacheLockKey, final String cacheLockValue) {
        try {
            final Long result = stringRedisTemplate.execute(
                    new DefaultRedisScript<>(luaScript, Long.class),
                    Collections.singletonList(cacheLockKey),
                    cacheLockValue
            );

            if (result == 0) {
                log.error("CRITICAL: Lock expired or ownership lost before release for key: {} - upstream load may have exceeded timeout", cacheLockKey);
            }
        } catch (Exception e) {
            log.error("Failed to delete Redis lock for key: {}, lock will auto-expire in {} seconds", cacheLockKey, upstreamDataLoadLockTimeout.getSeconds(), e);
        }
    }
}
