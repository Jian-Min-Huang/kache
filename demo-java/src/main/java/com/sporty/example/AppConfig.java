package com.sporty.example;

import com.sporty.core.SCache;
import com.sporty.core.SCacheSynchronizer;
import com.sporty.core.RedisPubSubSynchronizer;
import com.sporty.core.SCacheImpl;
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
    public SCacheSynchronizer sCacheSynchronizer(
            final RedisConnectionFactory redisConnectionFactory,
            final StringRedisTemplate stringRedisTemplate
    ) {
        return new RedisPubSubSynchronizer(redisConnectionFactory, stringRedisTemplate);
    }

    @Bean
    public SCache<Member> memberCache(
            final StringRedisTemplate stringRedisTemplate,
            final MemberRepository memberRepository,
            final SCacheSynchronizer sCacheSynchronizer
    ) {
        return new SCacheImpl<>(
                Member.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                stringRedisTemplate,
                Duration.ofMinutes(1),
                id -> memberRepository.findById(id).orElse(null),
                sCacheSynchronizer);
    }
}
