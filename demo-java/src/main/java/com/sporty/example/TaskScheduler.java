package com.sporty.example;

import com.sporty.kache.Kache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaskScheduler {
    private final Kache<Member> memberKache;

    @Scheduled(cron = "0 0/1 * * * ?")
    public void refresh() {
        memberKache
                .localCacheKeys()
                .forEach(key -> {
                    try {
                        memberKache.refresh(key);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
    }
}
