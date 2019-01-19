package ru.mail.polis.kirpichenkov;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.io.FileUtils;

/** @author Pavel Kirpichenkov */
public class KVDaoImpl implements BasePathGrantingKVDao {
  private static final Logger logger = LogManager.getLogger();
  private final File basePath;
  private final Map<Path, Boolean> filePresenceCache = FilePresenceCache.getInstance();
  public KVDaoImpl(@NotNull File path) {
    this.basePath = path;
  }

  @NotNull
  @Override
  public byte[] get(@NotNull final byte[] key) throws NoSuchElementException, IOException {
    File fileToRead = KeyConverter.keyToFile(key, basePath);
    logger.debug("get {}", fileToRead);
    if (!ExistsChecks.exists(fileToRead) || !fileToRead.isFile()) {
      throw new NoSuchElementException();
    } else {
      return IOUtils.toByteArray(new FileInputStream(fileToRead));
    }
  }

  @Override
  public void upsert(
      @NotNull final byte[] key,
      @NotNull final byte[] value
  ) throws IOException
  {
    File fileToWrite = KeyConverter.keyToFile(key, basePath);
    logger.debug("upsert {}", () -> fileToWrite);
    File parentDir = fileToWrite.getParentFile();
    if (!parentDir.exists()) {
      if (!parentDir.mkdirs() && !parentDir.exists()) {
        throw new IOException("Can't create path to file " + parentDir.toString());
      }
    }
    filePresenceCache.remove(fileToWrite.toPath());
    fileToWrite.createNewFile();
    FileUtils.writeByteArrayToFile(fileToWrite, value);
  }

  @Override
  public void remove(@NotNull final byte[] key) throws IOException {
    File fileToRemove = KeyConverter.keyToFile(key, basePath);
    logger.debug("remove {}", () -> fileToRemove);
    if (ExistsChecks.exists(fileToRemove)) {
      filePresenceCache.remove(fileToRemove.toPath());
      if (!fileToRemove.delete()) {
        throw new IOException("Can't remove file " + fileToRemove.toString());
      }
    }
  }

  @Override
  public void close() {}

  @Override
  @NotNull
  public File getBasePath() {
    return basePath;
  }
}
