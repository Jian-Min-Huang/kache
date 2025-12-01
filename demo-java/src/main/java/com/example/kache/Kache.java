package com.example.kache;

import java.io.IOException;
import java.util.Optional;

public abstract class Kache<T> {

    protected final String identifier;

    protected Kache(final String identifier) {
        this.identifier = identifier;
    }

    protected String buildKacheKey(final String key) {
        return "KACHE:%s:%s".formatted(identifier, key);
    }

    // 先查本地快取
    //   如果有，直接回傳
    //   如果沒有，再查 Redis
    //     如果有，寫本地快取，然後回傳
    //     如果沒有，再拿分布式鎖
    //       如果拿到鎖，去上游拿資料，更新  Redis，通知所有節點清除 Caffeine，然後回傳
    //       如果沒拿到鎖，回傳空
    public abstract Optional<T> getIfPresent(final String key);

    // 更新 Redis，通知所有節點清除 Caffeine
    public abstract void put(final String key, final T data) throws IOException;

    // 收到通知，清除 Caffeine
    public abstract void invalidateLocalCache(final String kacheKey);

    // 刪除 Redis，通知所有節點清除 Caffeine
    public abstract void invalidateAllCache(final String key) throws IOException;

    // 已知 key，從上游拿資料，更新 Redis，
    public abstract void refresh(final String key) throws IOException;
}
