package com.example.kache;

public interface KacheSynchronizer {
	void publishCacheInvalidation(final String kacheKey);

	void handleCacheInvalidation(final String cacheKey);

	<T> void registerKache(final String identifier, final Kache<T> kache);
}
