package com.example.kache;

import java.util.Optional;

public abstract class Kache<T> {

  protected final String identifier;

  protected Kache(final String identifier) {
    this.identifier = identifier;
  }

  public abstract Optional<T> getIfPresent(final String key);

  public abstract Boolean put(final String key, final T data);

  public abstract void invalidateLocalCache(final String kacheKey);

  protected String buildKacheKey(final String key) {
    return "KACHE:%s:%s".formatted(identifier, key);
  }

  public T getOrDefault(final String key, final T defaultValue) {
    return getIfPresent(key).orElse(defaultValue);
  }
}
