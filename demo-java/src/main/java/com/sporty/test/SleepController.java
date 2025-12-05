package com.sporty.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api")
public class SleepController {
    @GetMapping("/sleep/{timeout}")
    public String sleep(@PathVariable Long timeout) throws InterruptedException {
        Thread.sleep(timeout);
        return "Sleep Endpoint 1 -> Slept for " + timeout + " milliseconds";
    }
}
