package com.sporty.example;

import com.sporty.core.SCache;
import com.sporty.core.SCacheSynchronizerDefaultImpl;
import com.sporty.core.SCacheDefaultImpl;
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
    public SCache<Member> memberCache(
            final StringRedisTemplate stringRedisTemplate,
            final MemberRepository memberRepository,
            final RedisConnectionFactory redisConnectionFactory
    ) {
        return new SCacheDefaultImpl<>(
                Member.class,
                Duration.ofMinutes(5),
                1024L,
                Duration.ofMinutes(10),
                stringRedisTemplate,
                Duration.ofMinutes(1),
                id -> memberRepository.findById(id).orElse(null),
                8,
                16,
                60L,
                16,
                new SCacheSynchronizerDefaultImpl(stringRedisTemplate, redisConnectionFactory));
    }
}
