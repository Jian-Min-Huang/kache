package com.example.member;

import com.example.kache.Kache;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URL;

@RestController
@RequestMapping("/api/members")
@RequiredArgsConstructor
public class MemberController {
    private final Kache<MemberData> memberKache;
    private final MemberRepository memberRepository;

    @PostMapping("/{id}")
    public ResponseEntity<Void> upsert(@PathVariable("id") final String id, @RequestBody final MemberData memberData) {
        // Save to upstream
        memberRepository.save(id, memberData);
        // Update Kache
        try {
            memberKache.put(id, memberData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ResponseEntity.created(null).build();
    }

    @GetMapping("/{id}")
    public ResponseEntity<MemberData> queryById(@PathVariable("id") final String id) {
        return memberKache
                .getIfPresent(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> removeById(@PathVariable("id") final String id) {
        memberRepository.delete(id);
        try {
            memberKache.invalidateAllCache(id);
        } catch (IOException e) {
            // handle exception
            throw new RuntimeException(e);
        }

        return ResponseEntity.noContent().build();
    }
}
