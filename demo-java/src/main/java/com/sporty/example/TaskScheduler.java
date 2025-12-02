package com.sporty.example;

import com.sporty.kache.Kache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.stream.IntStream;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaskScheduler {
    private final Kache<Member> memberKache;

    @Scheduled(cron = "0 0/1 * * * ?")
    public void refresh() {
        IntStream
                .range(1, 3)
                .forEach(i -> {
                    try {
                        memberKache.refresh(String.valueOf(i));
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                });
    }
}
