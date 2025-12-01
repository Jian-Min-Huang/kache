package com.example.kache;

import java.io.IOException;
import java.util.Optional;

/**
 * Sample Code:
 * <pre>
 * &#64;Bean
 * public KacheSynchronizer kacheSynchronizer(
 *     final RedisConnectionFactory redisConnectionFactory,
 *     final StringRedisTemplate stringRedisTemplate
 * ) {
 *     return new RedisPubSubSynchronizer(redisConnectionFactory, stringRedisTemplate);
 * }
 *
 * &#64;Bean
 * public Kache&lt;MemberData&gt; memberKache(
 *     final StringRedisTemplate stringRedisTemplate,
 *     final MemberRepository memberRepository,
 *     final KacheSynchronizer kacheSynchronizer
 * ) {
 *     return new KacheImpl&lt;&gt;(
 *         Member.class,
 *         Duration.ofMinutes(5),
 *         1024L,
 *         Duration.ofMinutes(10),
 *         stringRedisTemplate,
 *         id -> memberRepository.findById(id).orElse(null),
 *         kacheSynchronizer);
 * }
 * </pre>
 */
public abstract class Kache<T> {
    protected final String identifier;

    private static final String KACHE_KEY_PREFIX = "KACHE";

    protected Kache(final String identifier) {
        this.identifier = identifier;
    }

    protected String buildKacheKey(final String key) {
        return "%s:%s:%s".formatted(KACHE_KEY_PREFIX, identifier, key);
    }

    /**
     * Sample Code:
     * <pre>
     * memberKache.getIfPresent(id).orElse(null);
     * </pre>
     *
     * 在拿不到互斥鎖的情況下，目前會直接回傳空
     * 後續可以考慮改成回傳預設值或是重試機制
     *
     * 所以如果我們拿到 Optional.empty()，可能有以下幾種情況
     * 1. Caffeine Cache 未命中，Redis Cache 未命中，沒拿到互斥鎖
     * 2. Caffeine Cache 未命中，Redis Cache 未命中，拿到互斥鎖，Upstream 無此資料
     * 2. Caffeine Cache 未命中，Redis Cache 未命中，拿到互斥鎖，但 Upstream 失敗
     */
    public abstract Optional<T> getIfPresent(final String key);

    /**
     * Sample Code:
     * <pre>
     * memberRepository.save(member);
     * try {
     *     memberKache.put(member.getId(), member);
     * } catch (IOException e) {
     *     // handle serialization exception
     *     // handle redis exception
     * }
     * </pre>
     *
     * 我們目前的設計理念是讓呼叫端自行處理序列化錯誤與 Redis 錯誤，並自行實作補償機制
     */
    public abstract void put(final String key, final T data) throws IOException;

    /**
     * Received notification (Redis Pub/Sub) then clear local cache (Caffeine).
     */
    public abstract void invalidateLocalCache(final String kacheKey);

    /**
     * Sample Code:
     * <pre>
     * memberRepository.delete(id);
     * try {
     *     memberKache.invalidateAllCache(id);
     * } catch (IOException e) {
     *     log.error(e.getMessage(), e);
     * }
     * </pre>
     *
     * 後續可以考慮是否要實現空值物件模式 (Null Object Pattern) 來避免頻繁的刪除操作以及 cache miss 問題
     */
    public abstract void invalidateAllCache(final String key) throws IOException;

    /**
     * Sample Code:
     * <pre>
     * final List&lt;String&gt; warmUpMemberIds = ...
     * for (String memberId : memberIds) {
     *     try {
     *         memberKache.refresh(memberId);
     *     } catch (IOException e) {
     *         log.error(e.getMessage(), e);
     *     }
     * }
     * </pre>
     */
    public abstract void refresh(final String key) throws IOException;
}
