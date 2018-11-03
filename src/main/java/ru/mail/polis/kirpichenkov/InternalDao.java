package ru.mail.polis.kirpichenkov;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.NoSuchElementException;

public class InternalDao {
  private static Logger logger = Logger.getLogger(InternalDao.class);
  private BasePathGrantingKVDao dao;

  InternalDao(BasePathGrantingKVDao dao) {
    this.dao = dao;
  }

  /**
   * Get object from storage by key If tombstone exists before get, file considered deleted If file
   * not found and tombstone exists, file also considered deleted If file not found and tombstone
   * does not exit, file considered absent If file retrieved successfully retrieved after check, it
   * is returned If IOError happened, error status is returned
   *
   * @param id key of value we want to retrieve
   * @return Result object with the result of operation
   */
  public Result get(byte[] id) {
    Result result = new Result();
    try {
      if (setResultIfDeleted(result, id)) {
        return result;
      }
      byte[] value = dao.get(id);
      result.setBody(value);
      result.setStatus(Result.Status.OK);
      Path file = KeyConverter.keyToFile(id, dao.getBasePath()).toPath();
      Instant timestamp = Files.getLastModifiedTime(file).toInstant();
      result.setTimestamp(timestamp);
    } catch (NoSuchElementException ex) {
      if (setResultIfDeleted(result, id)) {
        return result;
      } else {
        result.setStatus(Result.Status.ABSENT);
        result.setTimestamp(Instant.MIN);
      }
    } catch (IOException ex) {
      logger.error(ex);
      return error(result);
    }
    return result;
  }

  private boolean setResultIfDeleted(Result result, byte[] id) {
    File tombstone = KeyConverter.keyToTombstone(id, dao.getBasePath());
    if (!tombstone.exists()) {
      return false;
    }
    try {
      Instant lastModified = Files.getLastModifiedTime(tombstone.toPath()).toInstant();
      result.setTimestamp(lastModified);
      result.setStatus(Result.Status.DELETED);
      return true;
    } catch (Exception ex) {
      logger.error(ex);
      error(result);
      return true;
    }
  }

  public Result upsert(byte[] id, byte[] body) {
    Result result = new Result();
    try {
      Path filePath = KeyConverter.keyToFile(id, dao.getBasePath()).toPath();
      Path tombstonePath = KeyConverter.keyToTombstone(id, dao.getBasePath()).toPath();
      dao.upsert(id, body);
      Instant now = (new NanoClock()).instant();
      Files.setLastModifiedTime(filePath, FileTime.from(now));
      result.setStatus(Result.Status.OK);
      result.setTimestamp(now);
      checkAndRemoveTombstone(tombstonePath, now);
      return result;
    } catch (IOException ex) {
      logger.error(ex);
      return error(result);
    }
  }

  public Result remove(byte[] id) {
    Result result = new Result();
    try {
      File fileToRemove = KeyConverter.keyToFile(id, dao.getBasePath());
      if (!fileToRemove.exists()) {
        result.setStatus(Result.Status.OK);
        result.setTimestamp(Instant.now(new NanoClock()));
        return result;
      }
      String prefix = new String(id);
      Path tmpTombstonePath = Files.createTempFile(prefix, null);
      Path actualTombstonePath = KeyConverter.keyToTombstone(id, dao.getBasePath()).toPath();
      Instant now = (new NanoClock()).instant();
      Files.setLastModifiedTime(tmpTombstonePath, FileTime.from(now));
      if (checkAndMoveTombstone(tmpTombstonePath, actualTombstonePath, now)) {
        dao.remove(id);
        result.setTimestamp(now);
        result.setStatus(Result.Status.OK);
        return result;
      } else {
        return error(result);
      }
    } catch (IOException ex) {
      logger.error(ex);
      return error(result);
    }
  }

  private synchronized boolean checkAndMoveTombstone(Path from, Path to, Instant newTimestamp) {
    try {
      if (!Files.exists(to)) {
        Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        return true;
      } else if (Files.getLastModifiedTime(to).toInstant().isBefore(newTimestamp)) {
        Files.setLastModifiedTime(to, FileTime.from(newTimestamp));
        return true;
      } else {
        return false;
      }
    } catch (IOException ex) {
      logger.error(ex);
      return false;
    }
  }

  private synchronized void checkAndRemoveTombstone(Path path, Instant timestamp)
      throws IOException {
    if (Files.exists(path) && Files.getLastModifiedTime(path).toInstant().isBefore(timestamp)) {
      try {
        Files.delete(path);
      } catch (NoSuchFileException ex) {
        logger.warn(ex);
      }
    }
  }

  @NotNull
  private Result error(@NotNull Result result) {
    result.setTimestamp(Instant.MIN);
    result.setStatus(Result.Status.ERROR);
    return result;
  }
}
