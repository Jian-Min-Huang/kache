package com.example.kache;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RedisPubSubSynchronizer implements KacheSynchronizer {
    private final StringRedisTemplate stringRedisTemplate;
    private final RedisConnectionFactory redisConnectionFactory;

    private final Map<String, Kache<?>> registeredKaches = new ConcurrentHashMap<>();
    private final static String INVALIDATION_CHANNEL = "KACHE:INVALIDATION";

    private RedisMessageListenerContainer listenerContainer;

    public RedisPubSubSynchronizer(final StringRedisTemplate stringRedisTemplate, final RedisConnectionFactory redisConnectionFactory) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redisConnectionFactory = redisConnectionFactory;
    }

    @PostConstruct
    void start() {
        listenerContainer = new RedisMessageListenerContainer();
        listenerContainer.setConnectionFactory(redisConnectionFactory);
        listenerContainer.addMessageListener(
                (message, pattern) -> {
                    final String kacheKey = new String(message.getBody(), StandardCharsets.UTF_8);
                    handleCacheInvalidation(kacheKey);
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
    public void publishCacheInvalidation(final String kacheKey) {
        stringRedisTemplate.convertAndSend(INVALIDATION_CHANNEL, kacheKey);
    }

    @Override
    public void handleCacheInvalidation(final String kacheKey) {
        final String identifier = kacheKey.split(":", 3)[1];
        registeredKaches
                .forEach((registeredIdentifier, kache) -> {
                    if (registeredIdentifier.equals(identifier)) {
                        kache.invalidateLocalCache(kacheKey);
                    }
                });
    }

    @Override
    public <T> void registerKache(final String identifier, final Kache<T> kache) {
        registeredKaches.put(identifier, kache);
    }
}