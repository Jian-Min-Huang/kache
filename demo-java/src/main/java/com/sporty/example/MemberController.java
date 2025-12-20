package com.sporty.example;

import com.sporty.core.SCache;
import com.sporty.exception.SCacheLocalCacheOperateException;
import com.sporty.exception.SCacheRemoteCacheOperateException;
import com.sporty.exception.SCacheSerializeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@Log4j2
@RestController
@RequestMapping("/api/members")
@RequiredArgsConstructor
public class MemberController {
    private final SCache<Member> memberCache;
    private final MemberRepository memberRepository;

    @PostMapping("/{id}")
    public ResponseEntity<Void> upsert(@PathVariable final String id, @RequestBody final Member memberData) {
        memberRepository.save(id, memberData);
        try {
            memberCache.put(id, memberData);
        } catch (SCacheSerializeException | SCacheRemoteCacheOperateException | SCacheLocalCacheOperateException e) {
            if (e instanceof SCacheSerializeException) {
                // TODO:
            }
            if (e instanceof SCacheRemoteCacheOperateException) {
                // TODO:
            }
            if (e instanceof SCacheLocalCacheOperateException) {
                // TODO:
            }
        }

        return ResponseEntity.created(null).build();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Member> queryById(@PathVariable final String id) {
        return memberCache
                .getIfPresent(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> removeById(@PathVariable final String id) {
        memberRepository.delete(id);
        try {
            memberCache.invalidateAllCache(id);
        } catch (SCacheRemoteCacheOperateException | SCacheLocalCacheOperateException e) {
            if (e instanceof SCacheRemoteCacheOperateException) {
                // TODO:
            }
            if (e instanceof SCacheLocalCacheOperateException) {
                // TODO:
            }
        }

        return ResponseEntity.noContent().build();
    }
}
