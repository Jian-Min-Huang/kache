package com.sporty.example;

import com.sporty.kache.Kache;
import com.sporty.kache.KacheImpl;
import com.sporty.kache.KacheSynchronizer;
import com.sporty.kache.RedisPubSubSynchronizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Duration;

@EnableScheduling
@Configuration
public class AppConfig {
    @Bean
    public KacheSynchronizer kacheSynchronizer(
            final RedisConnectionFactory redisConnectionFactory,
            final StringRedisTemplate stringRedisTemplate
    ) {
        return new RedisPubSubSynchronizer(redisConnectionFactory, stringRedisTemplate);
    }

    @Bean
    public Kache<Member> memberKache(
            final StringRedisTemplate stringRedisTemplate,
            final MemberRepository memberRepository,
            final KacheSynchronizer kacheSynchronizer
    ) {
        return new KacheImpl<>(
                Member.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                stringRedisTemplate,
                id -> memberRepository.findById(id).orElse(null),
                kacheSynchronizer);
    }
}
