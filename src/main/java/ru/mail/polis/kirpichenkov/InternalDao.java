package ru.mail.polis.kirpichenkov;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.*;

public class InternalDao {
  private static Logger logger = LogManager.getLogger(InternalDao.class);
  private BasePathGrantingKVDao dao;
  private final Map<Path, Boolean> filePresenceCache = FilePresenceCache.getInstance();

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
  @NotNull
  public Result get(final byte[] id) {
    Result result = new Result();
    try {
      if (setResultIfDeleted(result, id)) {
        return result;
      }
      byte[] value = dao.get(id);
      Path file = KeyConverter.keyToFile(id, dao.getBasePath()).toPath();
      Instant timestamp = Files.getLastModifiedTime(file).toInstant();
      result
          .setBody(value)
          .setStatus(Result.Status.OK)
          .setTimestamp(timestamp);
    } catch (NoSuchElementException ex) {
      if (setResultIfDeleted(result, id)) {
        return result;
      } else {
        result
            .setStatus(Result.Status.ABSENT)
            .setTimestamp(Instant.MIN);
      }
    } catch (IOException ex) {
      logger.error(ex);
      return error(result);
    }
    return result;
  }

  private boolean setResultIfDeleted(
      @NotNull final Result result,
      final byte[] id
  ) {
    File tombstone = KeyConverter.keyToTombstone(id, dao.getBasePath());
    Path tombPath = tombstone.toPath();
    synchronized (tombstone.toPath().toString().intern()) {
      if (!ExistsChecks.exists(tombstone)) {
        return false;
      }
      try {
        Instant lastModified = Files.getLastModifiedTime(tombPath).toInstant();
        result
            .setTimestamp(lastModified)
            .setStatus(Result.Status.DELETED);
        return true;
      } catch (Exception ex) {
        logger.error(ex);
        error(result);
        return true;
      }
    }
  }

  @NotNull
  public Result upsert(
      final byte[] id,
      final byte[] body
  ) {
    Result result = new Result();
    Path filePath = KeyConverter.keyToFile(id, dao.getBasePath()).toPath();
    Path tombstonePath = KeyConverter.keyToTombstone(id, dao.getBasePath()).toPath();
    synchronized (filePath.toString().intern()) {
      try {
        filePresenceCache.remove(filePath);
        dao.upsert(id, body);
        Instant now = (new NanoClock()).instant();
        Files.setLastModifiedTime(filePath, FileTime.from(now));
        result
            .setStatus(Result.Status.OK)
            .setTimestamp(now);
        checkAndRemoveTombstone(tombstonePath, now);
        return result;
      } catch (IOException ex) {
        logger.error(ex);
        return error(result);
      }
    }
  }

  @NotNull
  public Result remove(final byte[] id) {
    Result result = new Result();
    try {
      File fileToRemove = KeyConverter.keyToFile(id, dao.getBasePath());
      if (!ExistsChecks.exists(fileToRemove)) {
        result
            .setStatus(Result.Status.OK)
            .setTimestamp(Instant.now(new NanoClock()));
        return result;
      }
      String prefix = new String(id);
      Path tmpTombstonePath = Files.createTempFile(prefix, null);
      Path actualTombstonePath = KeyConverter.keyToTombstone(id, dao.getBasePath()).toPath();
      Instant now = (new NanoClock()).instant();
      Files.setLastModifiedTime(tmpTombstonePath, FileTime.from(now));
      if (checkAndMoveTombstone(tmpTombstonePath, actualTombstonePath, now)) {
        filePresenceCache.remove(fileToRemove.toPath());
        dao.remove(id);
        result
            .setTimestamp(now)
            .setStatus(Result.Status.OK);
        return result;
      } else {
        return error(result);
      }
    } catch (IOException ex) {
      logger.error(ex);
      return error(result);
    }
  }

  private boolean checkAndMoveTombstone(
      @NotNull final Path from,
      @NotNull final Path to,
      @NotNull final Instant newTimestamp
  ) {
    synchronized (to.toString().intern()) {
      try {
        if (!ExistsChecks.exists(to)) {
          filePresenceCache.remove(to);
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
  }

  private void checkAndRemoveTombstone(
      @NotNull final Path path,
      @NotNull final Instant timestamp
  ) throws IOException
  {
    if (ExistsChecks.exists(path)
        && Files.getLastModifiedTime(path).toInstant().isBefore(timestamp))
    {
      try {
        filePresenceCache.remove(path);
        Files.delete(path);
      } catch (NoSuchFileException ex) {
        logger.warn(ex);
      }
    }
  }

  @NotNull
  private Result error(@NotNull Result result) {
    return result
        .setStatus(Result.Status.ERROR)
        .setTimestamp(Instant.MIN);
  }
}
