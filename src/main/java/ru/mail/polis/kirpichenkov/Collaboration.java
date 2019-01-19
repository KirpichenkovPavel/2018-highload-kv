package ru.mail.polis.kirpichenkov;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.kirpichenkov.Result.Status;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Collaboration {
  private static Logger logger = LogManager.getLogger(Collaboration.class);
  static final String INTERNAL_HEADER_KEY = "X-INTERNAL";
  static final String INTERNAL_HEADER_VALUE = "TRUE";
  static final String INTERNAL_HEADER =
      String.format("%s: %s", INTERNAL_HEADER_KEY, INTERNAL_HEADER_VALUE);
  static final String TIMESTAMP_HEADER = "X-TIMESTAMP";
  static final int STATUS_OK = 200;
  static final int STATUS_CREATED = 201;
  static final int STATUS_ACCEPTED = 202;
  static final int STATUS_NOT_FOUND = 404;
  static final int STATUS_ERROR = 500;
  static final int STATUS_NOT_ENOUGH_REPLICAS = 504;
  private static final long TIMEOUT = TimeUnit.MILLISECONDS.toMillis(500);
  private static final Map<String, HttpClient> connections = new HashMap<>();

  @NotNull
  static String entityPath(@NotNull final String id) {
    return "/v0/entity?id=" + id;
  }

  @NotNull
  public static Result error() {
    return new Result()
        .setStatus(Status.ERROR);
  }

  @NotNull
  static Result remote(
      @NotNull final Request request,
      @NotNull final String id,
      @NotNull final String nodeUrl
  ) {
    try {
      HttpClient client = connections.get(nodeUrl);
      if (client == null) {
        client = new HttpClient(new ConnectionString(nodeUrl + "?timeout=" + TIMEOUT));
        connections.put(nodeUrl, client);
      }
      switch (request.getMethod()) {
        case Request.METHOD_GET:
          return remoteGet(id, client);
        case Request.METHOD_PUT:
          return remotePut(id, client, request.getBody());
        case Request.METHOD_DELETE:
          return remoteDelete(id, client);
        default:
          return error();
      }
    } catch (InterruptedException | HttpException | IOException ex) {
      logger.error(ex);
      return error();
    } catch (PoolException ex) {
      logger.error("{}; Cause: {}", () -> ex, ex::getCause);
      return error();
    }
  }

  @NotNull
  static Result local(
      @NotNull final Request request,
      @NotNull final String id,
      @NotNull final InternalDao dao
  ) {
    switch (request.getMethod()) {
      case Request.METHOD_GET:
        return dao.get(id.getBytes());
      case Request.METHOD_PUT:
        return dao.upsert(id.getBytes(), request.getBody());
      case Request.METHOD_DELETE:
        return dao.remove(id.getBytes());
      default:
        return error();
    }
  }

  @NotNull
  private static Result remoteGet(
      @NotNull final String id,
      @NotNull final HttpClient client
  ) throws InterruptedException, HttpException, PoolException, IOException
  {
    Response response = client.get(entityPath(id), INTERNAL_HEADER);
    Instant timestamp = getTimestampFromHeader(response);
    Result result = new Result()
        .setTimestamp(timestamp);
    if (response.getStatus() == STATUS_OK) {
      result
          .setBody(response.getBody())
          .setStatus(Status.OK);
    } else if (response.getStatus() == STATUS_NOT_FOUND) {
      if (timestamp.equals(Instant.MIN)) {
        result.setStatus(Status.ABSENT);
      } else {
        result.setStatus(Status.DELETED);
      }
    }
    return result;
  }

  @NotNull
  private static Result remotePut(
      @NotNull final String id,
      @NotNull final HttpClient client,
      final byte[] body
  ) throws InterruptedException, HttpException, PoolException, IOException
  {
    Response response = client.put(entityPath(id), body, INTERNAL_HEADER);
    Instant timestamp = getTimestampFromHeader(response);
    Result result = new Result()
        .setTimestamp(timestamp);
    if (response.getStatus() == STATUS_CREATED) {
      result.setStatus(Status.OK);
    } else {
      result = error();
    }
    return result;
  }

  @NotNull
  private static Result remoteDelete(
      @NotNull final String id,
      @NotNull final HttpClient client
  ) throws InterruptedException, HttpException, PoolException, IOException
  {
    Response response = client.delete(entityPath(id), INTERNAL_HEADER);
    Instant timestamp = getTimestampFromHeader(response);
    Result result = new Result()
        .setTimestamp(timestamp);
    if (response.getStatus() == STATUS_ACCEPTED) {
      result.setStatus(Status.OK);
    } else {
      result = error();
    }
    return result;
  }

  static boolean isInternal(@NotNull final Request request) {
    String header = request.getHeader(INTERNAL_HEADER_KEY);
    return header != null && headerValue(header).equals(INTERNAL_HEADER_VALUE);
  }

  @NotNull
  private static Instant getTimestampFromHeader(
      @NotNull final Response response
  ) throws IllegalArgumentException
  {
    try {
      String timestamp = response.getHeader(TIMESTAMP_HEADER);
      if (timestamp != null) {
        return Instant.parse(headerValue(timestamp));
      } else {
        return Instant.MIN;
      }
    } catch (DateTimeParseException ex) {
      return Instant.MIN;
    }
  }

  @NotNull
  static Result mergeResults(
      @NotNull final Collection<Result> results,
      final int acksRequired
  ) {
    Result result = new Result()
        .setTimestamp(Instant.MIN)
        .setStatus(Status.ERROR);
    int successCounter = 0;
    for (Result r : results) {
      if (r.getStatus() != Status.ERROR) {
        successCounter += 1;
        if (result.getTimestamp().equals(Instant.MIN)
            || r.getTimestamp().isAfter(result.getTimestamp())) {
          result = r;
        }
      }
    }
    if (successCounter < acksRequired) {
      result.setStatus(Status.ERROR);
    }
    return result;
  }

  @NotNull
  private static String headerValue(@NotNull final String header) {
    if (!header.startsWith(":")) {
      throw new IllegalArgumentException("Unexpected raw header value: " + header);
    }
    return header.substring(1).trim();
  }
}
