package com.sporty.example;

import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class MemberRepository {
    final Map<String, Member> records = new ConcurrentHashMap<>();

    public MemberRepository() {
        records.put("1", new Member(1L, "name1"));
        records.put("2", new Member(2L, "name2"));
    }

    public Member save(final String id, final Member data) {
        return Optional
                .ofNullable(records.get(id))
                .map(v -> {
                    if (data.getName() != null) {
                        v.setName(data.getName());
                    }

                    return v;
                })
                .orElseGet(() -> {
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
