package com.sporty.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/await")
public class AwaitController {
    @GetMapping("/{timeout}")
    public String await(@PathVariable Long timeout) throws InterruptedException {
        Thread.sleep(timeout);
        return "/api/await/timeout -> Slept for " + timeout + " milliseconds";
    }
}
