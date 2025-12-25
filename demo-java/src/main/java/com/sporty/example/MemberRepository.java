package com.sporty.example;

import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Repository
public class MemberRepository {
    final Map<Long, Member> records = new ConcurrentHashMap<>();
    final AtomicLong id = new AtomicLong(1);

    public MemberRepository() {
        records.put(1L, new Member(id.getAndAdd(1L), "Vincent", 18, Instant.now(), Instant.now()));
        records.put(2L, new Member(id.getAndAdd(1L), "Alice", 25, Instant.now(), Instant.now()));
        records.put(3L, new Member(id.getAndAdd(1L), "Bob", 30, Instant.now(), Instant.now()));
        records.put(4L, new Member(id.getAndAdd(1L), "Charlie", 22, Instant.now(), Instant.now()));
        records.put(5L, new Member(id.getAndAdd(1L), "Diana", 28, Instant.now(), Instant.now()));
        records.put(6L, new Member(id.getAndAdd(1L), "Emma", 27, Instant.now(), Instant.now()));
        records.put(7L, new Member(id.getAndAdd(1L), "Frank", 35, Instant.now(), Instant.now()));
        records.put(8L, new Member(id.getAndAdd(1L), "Grace", 24, Instant.now(), Instant.now()));
        records.put(9L, new Member(id.getAndAdd(1L), "Henry", 29, Instant.now(), Instant.now()));
        records.put(10L, new Member(id.getAndAdd(1L), "Ivy", 26, Instant.now(), Instant.now()));
    }

    public Member save(final Member data) {
        return Optional
                .ofNullable(data.getId() == null ? null : records.get(data.getId()))
                // update
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
                // insert
                .orElseGet(() -> {
                    data.setId(id.getAndAdd(1L));
                    data.setCreateTime(Instant.now());
                    data.setUpdateTime(Instant.now());
                    records.put(data.getId(), data);
                    return data;
                });
    }

    public Optional<Member> findById(final Long id) {
        return Optional.ofNullable(records.get(id));
    }

    public void delete(final Long id) {
        records.remove(id);
    }
}
