package ru.mail.polis.kirpichenkov;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *  LRU cache for speeding up slow file presence checks
 */
class FilePresenceCache extends LinkedHashMap<Path, Boolean> {
  private FilePresenceCache() {
    // Singleton
  }

  private FilePresenceCache(
      final int initialSize,
      final float loadFactor,
      final boolean accessOrder
  ) {
    super(initialSize, loadFactor, accessOrder);
  }

  private static Map<Path, Boolean> instance;
  private static final int CACHE_SIZE = 128 * 1024;

  static synchronized Map<Path, Boolean> getInstance() {
    if (instance == null) {
      Cache<Path, Boolean> cache = Caffeine.newBuilder()
          .initialCapacity(CACHE_SIZE)
          .maximumSize(CACHE_SIZE)
          .build();
      instance = cache.asMap();
    }
    return instance;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<Path, Boolean> eldest) {
    return size() > CACHE_SIZE;
  }
}
