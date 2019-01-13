package ru.mail.polis.kirpichenkov;

import java.nio.file.Path;
import java.util.Collections;
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
      instance = Collections.synchronizedMap(
          new FilePresenceCache(CACHE_SIZE * 4/3, 0.75f, true)
      );
    }
    return instance;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<Path, Boolean> eldest) {
    return size() > CACHE_SIZE;
  }
}
