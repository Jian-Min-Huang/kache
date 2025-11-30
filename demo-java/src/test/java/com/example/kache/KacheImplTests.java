package com.example.kache;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mockito;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

class KacheImplTests {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  private static class TestData {
    private Long id;
    private String name;
  }

  private Kache<TestData> cache;
  private Cache<String, TestData> caffeineCache;
  private StringRedisTemplate redisTemplate;
  private ValueOperations<String, String> valueOps;
  private Function<String, TestData> upstream;
  private KacheSynchronizer kacheSynchronizer;

  private static class TestJsonException extends JsonProcessingException {
    TestJsonException(final String msg) {
      super(msg);
    }
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    caffeineCache = Mockito.mock(Cache.class);
    redisTemplate = Mockito.mock(StringRedisTemplate.class);
    valueOps = Mockito.mock(ValueOperations.class);
    upstream = Mockito.mock(Function.class);
    kacheSynchronizer = Mockito.mock(KacheSynchronizer.class);

    cache = new KacheImpl<>(
        "TestData",
        TestData.class,
        caffeineCache,
        redisTemplate,
        upstream,
        kacheSynchronizer);
  }

  @Test
  void getIfPresent_shouldReturnFromCaffeineWhenHit() {
    String key = "k1";
    String kacheKey = "KACHE:TestData:" + key;
    TestData member = new TestData(1L, "name1");
    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(member);

    Optional<TestData> result = cache.getIfPresent(key);

    assertThat(result).contains(member);
    verifyNoInteractions(redisTemplate, upstream);
  }

  @Test
  void getIfPresent_shouldReturnFromRedisWhenCaffeineMissAndRedisHit() {
    String key = "k1";
    String kacheKey = "KACHE:TestData:" + key;
    String json = "{\"id\":1,\"name\":\"name1\"}";
    TestData expected = new TestData(1L, "name1");

    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(null);
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
    String key = "k1";
    String kacheKey = "KACHE:TestData:" + key;
    String lockKey = kacheKey + ":lk";
    TestData member = new TestData(1L, "name1");

    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(null);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    when(valueOps.get(kacheKey)).thenReturn(null);
    when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
    when(upstream.apply(key)).thenReturn(member);

    Optional<TestData> result = cache.getIfPresent(key);

    assertThat(result).contains(member);
    verify(upstream).apply(key);
    verify(valueOps).set(eq(kacheKey), anyString());
    verify(caffeineCache).put(kacheKey, member);
    verify(redisTemplate).delete(lockKey);
  }

  @Test
  void getIfPresent_shouldReturnEmptyWhenLockNotAcquired() {
    String key = "k1";
    String kacheKey = "KACHE:TestData:" + key;
    String lockKey = kacheKey + ":lk";

    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(null);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    when(valueOps.get(kacheKey)).thenReturn(null);
    when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.FALSE);

    Optional<TestData> result = cache.getIfPresent(key);

    assertThat(result).isEmpty();
    verify(upstream, never()).apply(key);
  }

  @Test
  void put_shouldPropagateIOExceptionWhenSerializationFails() throws Exception {
    String key = "k1";
    TestData member = new TestData(1L, "name1");
    ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
    when(mapper.writeValueAsString(member)).thenThrow(new TestJsonException("boom"));

    Field objectMapperField = KacheImpl.class.getDeclaredField("objectMapper");
    objectMapperField.setAccessible(true);
    objectMapperField.set(cache, mapper);

    assertThatThrownBy(() -> cache.put(key, member))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Failed to serialize cache payload");

    verifyNoInteractions(redisTemplate, valueOps, caffeineCache, kacheSynchronizer);
  }

  @Test
  void put_shouldPropagateExceptionWhenRedisWriteFails() {
    String key = "k1";
    String kacheKey = "KACHE:TestData:" + key;
    TestData member = new TestData(1L, "name1");
    RuntimeException failure = new RuntimeException("redis down");

    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    doThrow(failure).when(valueOps).set(kacheKey, "{\"id\":1,\"name\":\"name1\"}");

    assertThatThrownBy(() -> cache.put(key, member))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Failed to write data to Redis cache for key");

    verify(redisTemplate).opsForValue();
    verify(valueOps).set(kacheKey, "{\"id\":1,\"name\":\"name1\"}");
    verifyNoInteractions(caffeineCache, kacheSynchronizer);
  }

  @Test
  void put_shouldWriteToRedisAndCaffeineAndPublishInvalidation() {
    String key = "k1";
    String kacheKey = "KACHE:TestData:" + key;
    TestData member = new TestData(1L, "name1");
    when(redisTemplate.opsForValue()).thenReturn(valueOps);

    assertThatCode(() -> cache.put(key, member)).doesNotThrowAnyException();

    verify(redisTemplate).opsForValue();
    verify(valueOps).set(kacheKey, "{\"id\":1,\"name\":\"name1\"}");
    verify(kacheSynchronizer).invalidateAllLocalCache(kacheKey);
  }
}
