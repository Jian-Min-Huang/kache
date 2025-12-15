package com.sporty.kache;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

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
     * memberRepository.save(member);
     * try {
     *     memberKache.put(member.getId(), member);
     * } catch (IOException e) {
     *     // handle serialization exception
     *     // handle redis exception
     * }
     * </pre>
     *
     * 這個方法通常會在資料變更後被呼叫，以確保 Cache 與資料庫的一致性
     *
     * 目前這個方法可能的結果有以下幾種:
     * 1. 成功寫入 Remote Cache 並通知刪除所有 Local Cache
     * 2. 序列化失敗，拋出 IOException
     * 3. 寫入 Remote Cache 失敗，拋出 IOException
     *
     * 我們目前的設計理念是讓呼叫端自行捕捉序列化錯誤與 Redis 錯誤，然後決定如何補償
     */
    public abstract void put(final String key, final T data) throws IOException;

    /**
     * Sample Code:
     * <pre>
     * memberKache.getIfPresent(id).orElse(null);
     * </pre>
     *
     * 目前這個方法可能的結果有以下幾種:
     * 1. Local Cache 命中，回傳資料
     * 2. Local Cache 未命中，Remote Cache 命中，回傳資料並更新 Local Cache
     * 3. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 有資料，回傳資料並更新 Remote 然後通知刪除所有 Local Cache
     * 4. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 有資料，但序列化或是寫入 Remote Cache 失敗，回傳 Optional.empty()
     * 5. Local Cache 未命中，Remote Cache 未命中，拿到互斥鎖，Upstream 無資料，回傳 Optional.empty()
     * 6. Local Cache 未命中，Remote Cache 未命中，沒拿到互斥鎖，回傳 Optional.empty()
     *
     * 在拿不到互斥鎖的情況下，目前會直接回傳空，可以考慮加上重試機制以提升命中率
     * 我們使用 Optional 來明確呼叫端需要處理資料不存在或是異常的情況, 4. 5. 6.
     */
    public abstract Optional<T> getIfPresent(final String key);

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
     * 這個方法通常會在資料刪除後被呼叫，以確保 Cache 與資料庫的一致性
     */
    public abstract void invalidateAllCache(final String key) throws IOException;

    /**
     * Received notification (Redis Pub/Sub) then clear local cache (Caffeine).
     */
    public abstract void invalidateLocalCache(final String kacheKey);

    /**
     * Acquire all local cache keys (Caffeine).
     */
    public abstract Set<String> localCacheKeys();

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
