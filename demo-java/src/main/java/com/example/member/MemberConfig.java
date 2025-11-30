package com.example.member;

import com.example.kache.Kache;
import com.example.kache.KacheImpl;
import com.example.kache.KacheSynchronizer;
import com.example.kache.RedisPubSubSynchronizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Duration;

@EnableScheduling
@Configuration
public class MemberConfig {
    @Bean
    public KacheSynchronizer kacheSynchronizer(final StringRedisTemplate stringRedisTemplate, final RedisConnectionFactory redisConnectionFactory) {
        return new RedisPubSubSynchronizer(stringRedisTemplate, redisConnectionFactory);
    }

    @Bean
    public Kache<MemberData> memberKache(
            final StringRedisTemplate stringRedisTemplate,
            final MemberRepository memberRepository,
            final KacheSynchronizer kacheSynchronizer) {
        return new KacheImpl<>(
                MemberData.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                stringRedisTemplate,
                id -> memberRepository.findById(id).orElse(null),
                kacheSynchronizer);
    }
}
