package com.sporty.kache;

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

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class KacheImplTests {
    private Kache<TestData> cache;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private Function<String, TestData> upstream;
    private KacheSynchronizer kacheSynchronizer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = Mockito.mock(StringRedisTemplate.class);
        valueOps = Mockito.mock(ValueOperations.class);
        upstream = Mockito.mock(Function.class);
        kacheSynchronizer = Mockito.mock(KacheSynchronizer.class);

        cache = new KacheImpl<>(
                TestData.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                redisTemplate,
                upstream,
                kacheSynchronizer
        );
    }

    @Test
    void getIfPresent_shouldReturnFromCaffeineWhenHit() throws Exception {
        String key = "1";
        String kacheKey = "KACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        TestData expected = new TestData(1L, "name1");

        Cache<String, TestData> mockCaffeineCache = Mockito.mock(Cache.class);
        when(mockCaffeineCache.getIfPresent(kacheKey)).thenReturn(expected);

        Field caffeineCacheField = KacheImpl.class.getDeclaredField("caffeineCache");
        caffeineCacheField.setAccessible(true);
        caffeineCacheField.set(cache, mockCaffeineCache);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verifyNoInteractions(redisTemplate, upstream);
    }

    @Test
    void getIfPresent_shouldReturnFromRedisWhenCaffeineMissAndRedisHit() {
        String key = "1";
        String kacheKey = "KACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String json = "{\"id\":1,\"name\":\"name1\"}";
        TestData expected = new TestData(1L, "name1");

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(kacheKey)).thenReturn(json);
        when(upstream.apply(key)).thenReturn(new TestData(99L, "fromUpstream"));

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);

        verify(redisTemplate).opsForValue();
        verify(valueOps).get(kacheKey);
        verify(upstream, never()).apply(key);
    }

    @Test
    void getIfPresent_shouldLoadFromUpstreamWhenCachesMissAndLockAcquired() {
        String key = "1";
        String kacheKey = "KACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String lockKey = kacheKey + ":lk";
        TestData expected = new TestData(1L, "name1");

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(kacheKey)).thenReturn(null);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
        when(upstream.apply(key)).thenReturn(expected);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).contains(expected);
        verify(upstream).apply(key);
        verify(valueOps).set(eq(kacheKey), anyString(), any(Duration.class));
        verify(redisTemplate).delete(lockKey);
    }

    @Test
    void getIfPresent_shouldReturnEmptyWhenLockNotAcquired() {
        String key = "1";
        String kacheKey = "KACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        String lockKey = kacheKey + ":lk";

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get(kacheKey)).thenReturn(null);
        when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.FALSE);

        Optional<TestData> result = cache.getIfPresent(key);

        assertThat(result).isEmpty();
        verify(upstream, never()).apply(key);
    }

    @Test
    void put_shouldPropagateIOExceptionWhenSerializationFails() throws Exception {
        String key = "1";
        TestData expected = new TestData(1L, "name1");
        ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
        when(mapper.writeValueAsString(expected)).thenThrow(new MockJsonProcessingException("boom"));

        Field objectMapperField = KacheImpl.class.getDeclaredField("objectMapper");
        objectMapperField.setAccessible(true);
        objectMapperField.set(cache, mapper);

        assertThatThrownBy(() -> cache.put(key, expected))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to serialize cache payload");

        verifyNoInteractions(redisTemplate, valueOps);
    }

    @Test
    void put_shouldPropagateExceptionWhenRedisWriteFails() {
        String key = "1";
        String kacheKey = "KACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        TestData expected = new TestData(1L, "name1");
        RuntimeException failure = new RuntimeException("redis down");

        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        doThrow(failure).when(valueOps).set(eq(kacheKey), eq("{\"id\":1,\"name\":\"name1\"}"), any(Duration.class));

        assertThatThrownBy(() -> cache.put(key, expected))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to write data to Redis cache for key");

        verify(redisTemplate).opsForValue();
        verify(valueOps).set(eq(kacheKey), eq("{\"id\":1,\"name\":\"name1\"}"), any(Duration.class));
    }

    @Test
    void put_shouldWriteToRedisAndCaffeineAndPublishInvalidation() {
        String key = "1";
        String kacheKey = "KACHE:%s:%s".formatted(TestData.class.getSimpleName(), key);
        TestData expected = new TestData(1L, "name1");
        when(redisTemplate.opsForValue()).thenReturn(valueOps);

        assertThatCode(() -> cache.put(key, expected)).doesNotThrowAnyException();

        verify(redisTemplate).opsForValue();
        verify(valueOps).set(eq(kacheKey), eq("{\"id\":1,\"name\":\"name1\"}"), any(Duration.class));
        verify(kacheSynchronizer).invalidateAllLocalCache(kacheKey);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class TestData {
        private Long id;
        private String name;
    }

    private static class MockJsonProcessingException extends JsonProcessingException {
        MockJsonProcessingException(final String msg) {
            super(msg);
        }
    }
}
