package com.sporty.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
class SCacheDefaultImplTests {
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

        cache = new SCacheDefaultImpl<>(
                TestData.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                redisTemplate,
                Duration.ofMinutes(1),
                upstream,
                8,
                16,
                60L,
                16,
                sCacheSynchronizer
        );
    }

    String key = "1";
    String sCacheKey = "SCACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
    String json = "{\"id\":1,\"name\":\"name1\"}";
    TestData expected = new TestData(1L, "name1");
    String lockKey = "SCACHE:%s:lock:%s".formatted(TestData.class.getSimpleName(), key);
    TestData upstreamData = new TestData(1L, "name1");

    /*
     *       L1 hit, L2 hit, Get lock, 2nd L2 hit, upstream empty, upstream error, upstream return, json error, L2 error, L1 error
     *  1         v,      -,        -,          -,             -,               -,               -,          -,        -,        -
     *  2         x,      v,        -,          -,             -,               -,               -,          -,        -,        -
     *  3         x,      x,        v,          v,             -,               -,               -,          -,        -,        -
     *  4         x,      x,        v,          x,             v,               -,               -,          -,        -,        -
     *  5         x,      x,        v,          x,             x,               v,               -,          -,        -,        -
     *  6         x,      x,        v,          x,             x,               x,               v,          v,        -,        -
     *  7         x,      x,        v,          x,             x,               x,               v,          x,        v,        -
     *  8         x,      x,        v,          x,             x,               x,               v,          x,        x,        v
     *  9         x,      x,        v,          x,             x,               x,               v,          x,        x,        x
     * 10         x,      x,        x,          -,             -,               -,               -,          -,        -,        -
     */

    @Test
    void getIfPresent_case1() throws Exception {
        Cache<String, TestData> mockCaffeineCache = Mockito.mock(Cache.class);
        when(mockCaffeineCache.getIfPresent(sCacheKey)).thenReturn(expected);
        Field caffeineCacheField = SCacheDefaultImpl.class.getDeclaredField("caffeineCache");
        caffeineCacheField.setAccessible(true);
        caffeineCacheField.set(cache, mockCaffeineCache);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verifyNoInteractions(redisTemplate, valueOps, upstream);
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case2() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(sCacheKey)).thenReturn(json);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verify(redisTemplate).opsForValue();
        verify(valueOps).get(sCacheKey);
        verify(valueOps, never()).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream, never()).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate, never()).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case3() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(sCacheKey)).thenReturn(null).thenReturn(json);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verify(redisTemplate, times(3)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream, never()).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case4() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(null);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();
        verify(redisTemplate, times(3)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }


    @Test
    void getIfPresent_case5() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenThrow(new RuntimeException("Upstream failure"));

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();
        verify(redisTemplate, times(3)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case6() throws Exception {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(upstreamData);
        ObjectMapper mockObjectMapper = Mockito.mock(ObjectMapper.class);
        when(mockObjectMapper.writeValueAsString(any())).thenThrow(new JsonProcessingException("Serialization failure") {});
        Field objectMapperField = SCacheDefaultImpl.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(cache, mockObjectMapper);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();
        verify(redisTemplate, times(3)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(mockObjectMapper).writeValueAsString(upstreamData);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case7() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(upstreamData);
        doThrow(new RuntimeException("Redis set failure")).when(valueOps).set(eq(sCacheKey), anyString(), any(Duration.class));

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();
        verify(redisTemplate, times(4)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer, never()).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case8() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(upstreamData);
        doThrow(new RuntimeException("Synchronizer invalidate failure")).when(sCacheSynchronizer).invalidateAllLocalCache(sCacheKey);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(upstreamData);
        verify(redisTemplate, times(4)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case9() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(expected);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verify(redisTemplate, times(4)).opsForValue();
        verify(valueOps, times(2)).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream).apply(key);
        verify(valueOps).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).execute(any(), anyList(), anyString());
        verify(sCacheSynchronizer).registerSCache(TestData.class.getTypeName(), cache);
        verify(sCacheSynchronizer).invalidateAllLocalCache(sCacheKey);
    }

    @Test
    void getIfPresent_case10() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.FALSE);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();
        verify(redisTemplate, times(2)).opsForValue();
        verify(valueOps).get(sCacheKey);
        verify(valueOps).setIfAbsent(eq(lockKey), anyString(), any(Duration.class));
        verify(upstream, never()).apply(key);
        verify(valueOps, never()).set(eq(sCacheKey), anyString(), any(Duration.class));
        verify(redisTemplate, never()).execute(any(), anyList(), anyString());
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
