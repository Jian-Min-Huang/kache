package com.example.member;

import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class MemberRepository {
    final Map<String, MemberData> store = new ConcurrentHashMap<>();

    public MemberRepository() {
        store.put("1", new MemberData(1L, "name1"));
        store.put("2", new MemberData(2L, "name2"));
    }

    public Optional<MemberData> findById(final String id) {
        return Optional.ofNullable(store.get(id));
    }

    public MemberData save(final String id, final MemberData data) {
        return Optional
                .ofNullable(store.get(id))
                .map(v -> {
                    if (data.getName() != null) {
                        v.setName(data.getName());
                    }

                    return v;
                })
                .orElseGet(() -> {
                    store.put(id, data);
                    return data;
                });
    }

    public void delete(final String id) {
        store.remove(id);
    }
}
