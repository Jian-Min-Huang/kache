package com.sporty.core;

import com.sporty.exception.SCacheLocalCacheOperateException;
import com.sporty.exception.SCacheRemoteCacheOperateException;
import com.sporty.exception.SCacheSerializeException;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * Sample Code:
 * <pre>
 * &#64;Bean
 * public SCacheSynchronizer sCacheSynchronizer(
 *     final RedisConnectionFactory redisConnectionFactory,
 *     final StringRedisTemplate stringRedisTemplate
 * ) {
 *     return new RedisPubSubSynchronizer(redisConnectionFactory, stringRedisTemplate);
 * }
 *
 * &#64;Bean
 * public SCache&lt;MemberData&gt; memberCache(
 *     final StringRedisTemplate stringRedisTemplate,
 *     final MemberRepository memberRepository,
 *     final SCacheSynchronizer sCacheSynchronizer
 * ) {
 *     return new SCacheImpl&lt;&gt;(
 *         Member.class,
 *         Duration.ofMinutes(5),
 *         1024L,
 *         Duration.ofMinutes(10),
 *         stringRedisTemplate,
 *         id -> memberRepository.findById(id).orElse(null),
 *         sCacheSynchronizer);
 * }
 * </pre>
 */
public abstract class SCache<T> {
    protected final String identifier;

    private static final String SCACHE_KEY_PREFIX = "SCACHE";

    protected SCache(final String identifier) {
        this.identifier = identifier;
    }

    protected String buildCacheKey(final String key) {
        return "%s:%s:%s".formatted(SCACHE_KEY_PREFIX, identifier, key);
    }

    protected String buildCacheLockKey(final String key) {
        return "%s:%s:lock:%s".formatted(SCACHE_KEY_PREFIX, identifier, key);
    }

    /**
     * Sample Code:
     * <pre>
     * memberRepository.save(member);
     * try {
     *     memberCache.put(member.getId(), member);
     * } catch (IOException e) {
     *     ...
     * }
     * </pre>
     *
     * This method is usually called after data changes to ensure consistency between the Cache and the database.
     *
     */
    public abstract void put(final String key, final T data) throws SCacheSerializeException, SCacheRemoteCacheOperateException, SCacheLocalCacheOperateException;

    /**
     * Sample Code:
     * <pre>
     * memberCache.getIfPresent(id).orElse(null);
     * </pre>
     */
    public abstract Optional<T> getIfPresent(final String key);

    /**
     * Sample Code:
     * <pre>
     * memberRepository.delete(id);
     * try {
     *     memberCache.invalidateAllCache(id);
     * } catch (IOException e) {
     *     log.error(e.getMessage(), e);
     * }
     * </pre>
     *
     * This method is usually called after data deletion to ensure consistency between the Cache and the database.
     */
    public abstract void invalidateAllCache(final String key) throws SCacheRemoteCacheOperateException, SCacheLocalCacheOperateException;

    /**
     * Received notification then clear local cache.
     */
    public abstract void invalidateLocalCache(final String sCacheKey);

    /**
     * Acquire all local cache keys.
     */
    public abstract Set<String> localCacheKeys();

    /**
     * Sample Code:
     * <pre>
     * final List&lt;String&gt; warmUpMemberIds = ...
     * for (String memberId : memberIds) {
     *     try {
     *         memberCache.refresh(memberId);
     *     } catch (IOException e) {
     *         log.error(e.getMessage(), e);
     *     }
     * }
     * </pre>
     */
    public abstract void refresh(final String key) throws IOException;
}
