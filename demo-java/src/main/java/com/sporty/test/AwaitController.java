package com.sporty.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api")
public class AwaitController {
    @GetMapping("/await/1/{timeout}")
    public String await1(@PathVariable Long timeout) throws InterruptedException {
        Thread.sleep(timeout);
        return "Await Endpoint 1 -> Slept for " + timeout + " milliseconds";
    }

    @GetMapping("/await/2/{timeout}")
    public String await2(@PathVariable Long timeout) throws InterruptedException {
        Thread.sleep(timeout);
        return "Await Endpoint 2 -> Slept for " + timeout + " milliseconds";
    }
}
