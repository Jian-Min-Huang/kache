package com.example.kache;

public interface KacheSynchronizer {
	void publishCacheInvalidation(final String cacheKey);

	void handleCacheInvalidation(final String cacheKey);

	<T> T registerKache(final String identifier, final Kache<T> kache);
}
