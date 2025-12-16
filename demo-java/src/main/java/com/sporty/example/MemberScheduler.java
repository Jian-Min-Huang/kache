package com.sporty.example;

import com.sporty.core.SCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MemberScheduler {
    private final SCache<Member> memberCache;

    @Scheduled(cron = "0 0/1 * * * ?")
    public void refresh() {
        memberCache
                .localCacheKeys()
                .forEach(key -> {
                    try {
                        memberCache.refresh(key);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
    }
}
