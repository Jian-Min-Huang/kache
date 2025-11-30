package com.example.member;

import com.example.kache.Kache;
import com.example.kache.KacheImpl;
import com.example.kache.KacheSynchronizer;
import com.example.kache.RedisPubSubSynchronizer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Duration;
import java.util.function.Function;

@EnableScheduling
@Configuration
public class MemberConfig {
    @Bean
    public Cache<String, MemberData> memberLocalCache() {
        return Caffeine
                .newBuilder()
                .expireAfterWrite(Duration.ofMinutes(5))
                .maximumSize(1024)
                .recordStats()
                .build();
    }

    @Bean
    public Function<String, MemberData> memberUpstreamDataLoader(final MemberRepository memberRepository) {
        return id -> memberRepository.findById(id).orElse(null);
    }

    @Bean
    public KacheSynchronizer kacheSynchronizer(final StringRedisTemplate stringRedisTemplate, final RedisConnectionFactory redisConnectionFactory) {
        return new RedisPubSubSynchronizer(stringRedisTemplate, redisConnectionFactory);
    }

    @Bean
    public Kache<MemberData> memberKache(
            final StringRedisTemplate stringRedisTemplate,
            final Function<String, MemberData> memberUpstreamDataLoader,
            final KacheSynchronizer kacheSynchronizer) {
        final String identifier = "MemberData";
        final Kache<MemberData> kache = new KacheImpl<>(
                identifier,
                MemberData.class,
                memberLocalCache(),
                stringRedisTemplate,
                memberUpstreamDataLoader,
                kacheSynchronizer);
        kacheSynchronizer.registerKache(identifier, kache);
        return kache;
    }
}
