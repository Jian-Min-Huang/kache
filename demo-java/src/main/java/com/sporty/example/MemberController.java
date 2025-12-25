package com.sporty.example;

import com.sporty.core.SCache;
import com.sporty.exception.SCacheBlankKeyException;
import com.sporty.exception.SCacheLocalCacheOperateException;
import com.sporty.exception.SCacheRemoteCacheOperateException;
import com.sporty.exception.SCacheSerializeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/members")
@RequiredArgsConstructor
public class MemberController {
    private final SCache<Member> memberCache;
    private final MemberRepository memberRepository;

    @PostMapping
    public ResponseEntity<Void> upsert(@RequestBody final Member memberData) {
        final Member save = memberRepository.save(memberData);
        try {
            memberCache.put(save.getId().toString(), memberData);
        } catch (SCacheBlankKeyException | SCacheSerializeException | SCacheRemoteCacheOperateException |
                 SCacheLocalCacheOperateException e) {
            if (e instanceof SCacheBlankKeyException) {
                // TODO:
                log.error(e.getMessage(), e);
            }
            if (e instanceof SCacheSerializeException) {
                // TODO:
                log.error(e.getMessage(), e);
            }
            if (e instanceof SCacheRemoteCacheOperateException) {
                // TODO:
                log.error(e.getMessage(), e);
            }
            if (e instanceof SCacheLocalCacheOperateException) {
                // TODO:
                log.error(e.getMessage(), e);
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
        memberRepository.delete(Long.parseLong(id));
        try {
            memberCache.invalidateAllCache(id);
        } catch (SCacheRemoteCacheOperateException | SCacheLocalCacheOperateException e) {
            if (e instanceof SCacheRemoteCacheOperateException) {
                // TODO:
                log.error(e.getMessage(), e);
            }
            if (e instanceof SCacheLocalCacheOperateException) {
                // TODO:
                log.error(e.getMessage(), e);
            }
        }

        return ResponseEntity.noContent().build();
    }
}
