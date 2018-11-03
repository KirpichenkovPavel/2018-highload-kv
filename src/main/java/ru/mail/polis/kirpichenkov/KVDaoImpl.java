package ru.mail.polis.kirpichenkov;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.commons.io.FileUtils;

/** @author Pavel Kirpichenkov */
public class KVDaoImpl implements KVDao, BasePathGrantingKVDao {
  private static final Logger logger = Logger.getLogger(KVDaoImpl.class);

  private final File basePath;

  public KVDaoImpl(@NotNull File path) {
    this.basePath = path;
  }

  @NotNull
  @Override
  public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
    File fileToRead = KeyConverter.keyToFile(key, basePath);
    logger.debug(String.format("get %s", fileToRead.toString()));
    if (!fileToRead.exists() || !fileToRead.isFile()) {
      throw new NoSuchElementException();
    } else {
      return IOUtils.toByteArray(new FileInputStream(fileToRead));
    }
  }

  @Override
  public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
    File fileToWrite = KeyConverter.keyToFile(key, basePath);
    logger.debug(String.format("upsert %s", fileToWrite));
    File parentDir = fileToWrite.getParentFile();
    if (!parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new IOException("Can't create path to file " + parentDir.toString());
      }
    }
    fileToWrite.createNewFile();
    FileUtils.writeByteArrayToFile(fileToWrite, value);
  }

  @Override
  public void remove(@NotNull byte[] key) throws IOException {
    File fileToRemove = KeyConverter.keyToFile(key, basePath);
    logger.debug(String.format("remove %s", fileToRemove));
    if (fileToRemove.exists()) {
      if (!fileToRemove.delete()) {
        throw new IOException("Can't remove file");
      }
    }
  }

  @Override
  public void close() throws IOException {}

  @Override
  public File getBasePath() {
    return basePath;
  }
}
