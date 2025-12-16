package com.sporty.core;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.nio.charset.StandardCharsets;

@Slf4j
public class RedisPubSubSynchronizer extends SCacheSynchronizer {
    private final RedisConnectionFactory redisConnectionFactory;
    private final StringRedisTemplate stringRedisTemplate;

    private final static String INVALIDATION_CHANNEL = "SCACHE:INVALIDATION";

    private RedisMessageListenerContainer listenerContainer;

    public RedisPubSubSynchronizer(
            final RedisConnectionFactory redisConnectionFactory,
            final StringRedisTemplate stringRedisTemplate
    ) {
        this.redisConnectionFactory = redisConnectionFactory;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @PostConstruct
    void start() {
        listenerContainer = new RedisMessageListenerContainer();
        listenerContainer.setConnectionFactory(redisConnectionFactory);
        listenerContainer.addMessageListener(
                (message, pattern) -> {
                    final String sCacheKey = new String(message.getBody(), StandardCharsets.UTF_8);
                    invalidateLocalCache(sCacheKey);
                },
                new ChannelTopic(INVALIDATION_CHANNEL));
        listenerContainer.afterPropertiesSet();
        listenerContainer.start();
    }

    @PreDestroy
    void shutdown() {
        if (listenerContainer != null && listenerContainer.isRunning()) {
            listenerContainer.stop();
        }
    }

    @Override
    public void invalidateAllLocalCache(final String sCacheKey) {
        stringRedisTemplate.convertAndSend(INVALIDATION_CHANNEL, sCacheKey);
    }

    @Override
    public void invalidateLocalCache(final String sCacheKey) {
        final String identifier = sCacheKey.split(":", 3)[1];
        registeredSCaches
                .forEach((registeredIdentifier, sCache) -> {
                    if (registeredIdentifier.equals(identifier)) {
                        sCache.invalidateLocalCache(sCacheKey);
                    }
                });
    }
}