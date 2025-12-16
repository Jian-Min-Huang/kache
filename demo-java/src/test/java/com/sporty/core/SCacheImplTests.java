package com.sporty.core;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class SCacheImplTests {
    private SCache<TestData> cache;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private Function<String, TestData> upstream;
    private SCacheSynchronizer sCacheSynchronizer;

    @BeforeEach
    void setUp() {
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        valueOps = Mockito.mock(ValueOperations.class);
        upstream = Mockito.mock(Function.class);
        sCacheSynchronizer = Mockito.mock(SCacheSynchronizer.class);

        cache = new SCacheImpl<>(
                TestData.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                redisTemplate,
                upstream,
                sCacheSynchronizer
        );
    }

    @Test
    void getIfPresent_shouldReturnFromCaffeineWhenHit() throws Exception {
        String key = "1";
        String sCacheKey = "SCACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        TestData expected = new TestData(1L, "name1");

        Cache<String, TestData> mockCaffeineCache = Mockito.mock(Cache.class);
        when(mockCaffeineCache.getIfPresent(sCacheKey)).thenReturn(expected);

        Field caffeineCacheField = SCacheImpl.class.getDeclaredField("caffeineCache");
        caffeineCacheField.setAccessible(true);
        caffeineCacheField.set(cache, mockCaffeineCache);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verifyNoInteractions(redisTemplate, valueOps, upstream);
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_shouldReturnFromRedisWhenCaffeineMissAndRedisHit() {
        String key = "1";
        String sCacheKey = "SCACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String json = "{\"id\":1,\"name\":\"name1\"}";
        TestData expected = new TestData(1L, "name1");

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(sCacheKey)).thenReturn(json);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);

        verify(redisTemplate).opsForValue();
        verify(valueOps).get(sCacheKey);
        verify(upstream, never()).apply(key);
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_shouldLoadFromUpstreamWhenCachesMissAndLockAcquiredButNoData() {
        String key = "1";
        String sCacheKey = "SCACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String lockKey = sCacheKey + ":lk";

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(null);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();

        verify(redisTemplate, times(2)).opsForValue();
        verify(valueOps, times(1)).get(sCacheKey);
        verify(valueOps, times(1)).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).delete(lockKey);
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_shouldLoadFromUpstreamWhenCachesMissAndLockAcquired() {
        String key = "1";
        String sCacheKey = "SCACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String lockKey = sCacheKey + ":lk";
        TestData expected = new TestData(1L, "name1");

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(expected);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);

        verify(redisTemplate, times(3)).opsForValue();
        verify(valueOps, times(1)).get(sCacheKey);
        verify(valueOps, times(1)).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps, times(1)).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).delete(lockKey);
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_shouldReturnEmptyWhenLockNotAcquired() {
        String key = "1";
        String sCacheKey = "SCACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String lockKey = sCacheKey + ":lk";

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.FALSE);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();

        verify(redisTemplate, times(2)).opsForValue();
        verify(valueOps, times(1)).get(sCacheKey);
        verify(valueOps, times(1)).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream, never()).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate, never()).delete(lockKey);
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class TestData {
        private Long id;
        private String name;
    }
}
