package com.sporty.example;

import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class MemberRepository {
    final Map<String, Member> records = new ConcurrentHashMap<>();

    public MemberRepository() {
        records.put("1", new Member(1L, "Vincent", 18, Instant.now(), Instant.now()));
        records.put("2", new Member(2L, "Alice", 25, Instant.now(), Instant.now()));
        records.put("3", new Member(3L, "Bob", 30, Instant.now(), Instant.now()));
        records.put("4", new Member(4L, "Charlie", 22, Instant.now(), Instant.now()));
        records.put("5", new Member(5L, "Diana", 28, Instant.now(), Instant.now()));
        records.put("6", new Member(6L, "Emma", 27, Instant.now(), Instant.now()));
        records.put("7", new Member(7L, "Frank", 35, Instant.now(), Instant.now()));
        records.put("8", new Member(8L, "Grace", 24, Instant.now(), Instant.now()));
        records.put("9", new Member(9L, "Henry", 29, Instant.now(), Instant.now()));
        records.put("10", new Member(10L, "Ivy", 26, Instant.now(), Instant.now()));
    }

    public Member save(final String id, final Member data) {
        return Optional
                .ofNullable(records.get(id))
                .map(v -> {
                    if (data.getName() != null) {
                        v.setName(data.getName());
                    }
                    if (data.getAge() != null) {
                        v.setAge(data.getAge());
                    }
                    v.setUpdateTime(Instant.now());

                    return v;
                })
                .orElseGet(() -> {
                    data.setCreateTime(Instant.now());
                    data.setUpdateTime(Instant.now());
                    records.put(id, data);
                    return data;
                });
    }

    public Optional<Member> findById(final String id) {
        return Optional.ofNullable(records.get(id));
    }

    public void delete(final String id) {
        records.remove(id);
    }
}
