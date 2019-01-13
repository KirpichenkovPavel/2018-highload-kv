package ru.mail.polis.kirpichenkov;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

class ExistsChecks {
  private ExistsChecks() {
    // Not instantiatable
  }

  static boolean exists(File file) {
    Path path = file.toPath();
    return exists(path);
  }

  static boolean exists(Path path) {
    Map<Path, Boolean> cache = FilePresenceCache.getInstance();
    Boolean entry = cache.get(path);
    if (entry != null) {
      return entry;
    }
    Boolean presence = Files.exists(path);
    cache.put(path, presence);
    return presence;
  }
}
