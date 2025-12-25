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
        return SCacheDefaultImpl
                .builder(Member.class)
                .localCacheExpiry(Duration.ofMinutes(5))
                .maximumSize(1024L)
                .remoteCacheExpiry(Duration.ofMinutes(10))
                .stringRedisTemplate(stringRedisTemplate)
                .upstreamDataLoadTimeout(Duration.ofMinutes(1))
                .upstreamDataLoadFunction(id -> memberRepository.findById(Long.parseLong(id)).orElse(null))
                .upstreamDataLoadPoolSize(8)
                .sCacheSynchronizer(new SCacheSynchronizerDefaultImpl(stringRedisTemplate, redisConnectionFactory))
                .build();
    }
}
