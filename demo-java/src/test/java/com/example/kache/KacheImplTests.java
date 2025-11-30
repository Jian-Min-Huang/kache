package com.example.kache;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mockito;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.github.benmanes.caffeine.cache.Cache;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

class KacheImplTests {

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  private static class TestMember {
    Long id;
    String name;
  }

  private Cache<String, TestMember> caffeineCache;
  private StringRedisTemplate redisTemplate;
  private ValueOperations<String, String> valueOps;
  private Supplier<TestMember> upstream;
  private Kache<TestMember> cache;
  private KacheSynchronizer kacheSynchronizer;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    caffeineCache = Mockito.mock(Cache.class);
    redisTemplate = Mockito.mock(StringRedisTemplate.class);
    valueOps = Mockito.mock(ValueOperations.class);
    upstream = Mockito.mock(Supplier.class);
    kacheSynchronizer = Mockito.mock(KacheSynchronizer.class);

    cache = new KacheImpl<>(
        "TestMember",
        TestMember.class,
        caffeineCache,
        redisTemplate,
        upstream,
        kacheSynchronizer);
  }

  @Test
  void getIfPresent_shouldReturnFromCaffeineWhenHit() {
    String key = "k1";
    String kacheKey = "KACHE:TestMember:" + key;
    TestMember member = new TestMember(1L, "m1");
    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(member);

    Optional<TestMember> result = cache.getIfPresent(key);

    assertThat(result).contains(member);
    verifyNoInteractions(redisTemplate, upstream);
  }

  @Test
  void getIfPresent_shouldReturnFromRedisWhenCaffeineMissAndRedisHit() throws Exception {
    String key = "k1";
    String kacheKey = "KACHE:TestMember:" + key;
    String json = "{\"id\":1,\"name\":\"m1\"}";
    TestMember expected = new TestMember(1L, "m1");

    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(null);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    when(valueOps.get(kacheKey)).thenReturn(json);
    when(upstream.get()).thenReturn(new TestMember(99L, "fromUpstream"));

    Optional<TestMember> result = cache.getIfPresent(key);

    assertThat(result).contains(expected);

    verify(redisTemplate).opsForValue();
    verify(valueOps).get(kacheKey);
    verify(upstream, never()).get();
  }

  @Test
  void getIfPresent_shouldLoadFromUpstreamWhenCachesMissAndLockAcquired() throws Exception {
    String key = "k1";
    String kacheKey = "KACHE:TestMember:" + key;
    String lockKey = kacheKey + ":lk";
    TestMember member = new TestMember(1L, "m1");

    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(null);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    when(valueOps.get(kacheKey)).thenReturn(null);
    when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.TRUE);
    when(upstream.get()).thenReturn(member);

    Optional<TestMember> result = cache.getIfPresent(key);

    assertThat(result).contains(member);
    verify(upstream).get();
    verify(valueOps).set(eq(kacheKey), anyString());
    verify(caffeineCache).put(kacheKey, member);
    verify(redisTemplate).delete(lockKey);
  }

  @Test
  void getIfPresent_shouldReturnEmptyWhenLockNotAcquired() {
    String key = "k1";
    String kacheKey = "KACHE:TestMember:" + key;
    String lockKey = kacheKey + ":lk";

    when(caffeineCache.getIfPresent(kacheKey)).thenReturn(null);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
    when(valueOps.get(kacheKey)).thenReturn(null);
    when(valueOps.setIfAbsent(eq(lockKey), anyString(), any(Duration.class))).thenReturn(Boolean.FALSE);

    Optional<TestMember> result = cache.getIfPresent(key);

    assertThat(result).isEmpty();
    verify(upstream, never()).get();
  }

  @Test
  void put_shouldWriteToRedisAndCaffeineAndPublishInvalidation() throws Exception {
    String key = "k1";
    String kacheKey = "KACHE:TestMember:" + key;
    TestMember member = new TestMember(1L, "m1");

    when(redisTemplate.opsForValue()).thenReturn(valueOps);

    Boolean result = cache.put(key, member);

    assertThat(result).isTrue();
    verify(valueOps).set(eq(kacheKey), anyString());
    verify(caffeineCache).put(kacheKey, member);
    verify(kacheSynchronizer).publishCacheInvalidation(kacheKey);
  }

  @Test
  void put_shouldReturnFalseAndNotTouchCaffeineOrSynchronizerWhenRedisFails() throws Exception {
    String key = "k1";
    String kacheKey = "KACHE:TestMember:" + key;
    TestMember member = new TestMember(1L, "m1");

    when(redisTemplate.opsForValue()).thenThrow(new RuntimeException("redis error"));

    Boolean result = cache.put(key, member);

    assertThat(result).isFalse();
    verify(caffeineCache, never()).put(eq(kacheKey), eq(member));
    verifyNoInteractions(kacheSynchronizer);
  }
}
