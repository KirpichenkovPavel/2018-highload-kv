package ru.mail.polis.kirpichenkov;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.commons.io.FileUtils;

/** @author Pavel Kirpichenkov */
public class KVDaoImpl implements KVDao {
  private final int HEX_CHAR_PER_DIR = 4;
  private static final Logger logger = Logger.getLogger(KVDaoImpl.class);

  private final File basePath;

  public KVDaoImpl(@NotNull File path) {
    this.basePath = path;
  }

  @NotNull
  @Override
  public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
    File fileToRead = keyToFile(key);
    logger.debug(String.format("get %s", fileToRead.toString()));
    if (!fileToRead.exists() || !fileToRead.isFile()) {
      throw new NoSuchElementException();
    } else {
      return IOUtils.toByteArray(new FileInputStream(fileToRead));
    }
  }

  @Override
  public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
    File fileToWrite = keyToFile(key);
    logger.debug(String.format("upsert %s", fileToWrite));
    File parentDir = fileToWrite.getParentFile();
    if (!parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new IOException("Can't create path to file");
      }
    }
    fileToWrite.createNewFile();
    FileUtils.writeByteArrayToFile(fileToWrite, value);
  }

  @Override
  public void remove(@NotNull byte[] key) throws IOException {
    File fileToRemove = keyToFile(key);
    logger.debug(String.format("upsert %s", fileToRemove));
    if (fileToRemove.exists()) {
      if (!fileToRemove.delete()) {
        throw new IOException("Can't remove file");
      }
    }
  }

  @Override
  public void close() throws IOException {}

  /**
   * Convert key from byte array to file path. The total number of subdirectories and files in one
   * directory is limited. Long keys are transformed into directory hierarchy
   */
  @NotNull
  private File keyToFile(@NotNull byte[] key) {
    final String hexKey = DatatypeConverter.printHexBinary(key);
    final int hexLength = hexKey.length();
    final StringBuilder path = new StringBuilder(basePath.toString());

    int ix;
    for (ix = 0; hexLength - ix > HEX_CHAR_PER_DIR; ix += HEX_CHAR_PER_DIR) {
      path.append('/').append(hexKey, ix, ix + HEX_CHAR_PER_DIR);
    }
    path.append('/').append(hexKey, ix, hexLength);
    return new File(path.toString());
  }
}
