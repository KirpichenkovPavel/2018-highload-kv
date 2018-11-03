package ru.mail.polis.kirpichenkov;

import org.jetbrains.annotations.NotNull;

import javax.xml.bind.DatatypeConverter;
import java.io.File;

class KeyConverter {
  private static final int HEX_CHAR_PER_DIR = 4;
  private static final String FILE_SUFFIX = "_";
  private static final String TOMBSTONE_SUFFIX = "+";

  private KeyConverter() {}

  /**
   * Convert key from byte array to file path. The total number of subdirectories and files in one
   * directory is limited. Long keys are transformed into directory hierarchy
   */
  @NotNull
  static File keyToFile(@NotNull byte[] key, File basePath) {
    return new File(pathString(key, basePath));
  }

  @NotNull
  static File keyToTombstone(@NotNull byte[] key, File basePath) {
    return new File(pathString(key, basePath) + TOMBSTONE_SUFFIX);
  }

  private static String pathString(@NotNull byte[] key, File basePath) {
    final String hexKey = DatatypeConverter.printHexBinary(key);
    final int hexLength = hexKey.length();
    final StringBuilder path = new StringBuilder(basePath.toString());

    int ix;
    for (ix = 0; hexLength - ix > HEX_CHAR_PER_DIR; ix += HEX_CHAR_PER_DIR) {
      path.append('/').append(hexKey, ix, ix + HEX_CHAR_PER_DIR);
    }
    path.append('/').append(hexKey, ix, hexLength).append(FILE_SUFFIX);
    return path.toString();
  }
}
