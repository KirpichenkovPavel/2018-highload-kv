package ru.mail.polis.kirpichenkov;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.kirpichenkov.Result.Status;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class Collaboration {
  private static Logger logger = Logger.getLogger(Collaboration.class);
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

  static String entityPath(final String id) {
    return "/v0/entity?id=" + id;
  }

  private static Result error() {
    Result result = new Result();
    result.setStatus(Status.ERROR);
    return result;
  }

  static Result remote(Request request, String id, String nodeUrl) {
    try (HttpClient client =
        new HttpClient(new ConnectionString(nodeUrl + "?timeout=" + TIMEOUT))) {
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
    } catch (InterruptedException | HttpException | PoolException | IOException ex) {
      logger.error(ex);
      return error();
    }
  }

  static Result local(Request request, String id, InternalDao dao) {
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

  private static Result remoteGet(String id, HttpClient client)
      throws InterruptedException, HttpException, PoolException, IOException {
    Result result = new Result();
    Response response = client.get(entityPath(id), INTERNAL_HEADER);
    Instant timestamp = getTimestampFromHeader(response);
    result.setTimestamp(timestamp);
    if (response.getStatus() == STATUS_OK) {
      result.setBody(response.getBody());
      result.setStatus(Status.OK);
    } else if (response.getStatus() == STATUS_NOT_FOUND) {
      if (timestamp.equals(Instant.MIN)) {
        result.setStatus(Status.ABSENT);
      } else {
        result.setStatus(Status.DELETED);
      }
    }
    return result;
  }

  private static Result remotePut(String id, HttpClient client, byte[] body)
      throws InterruptedException, HttpException, PoolException, IOException {
    Result result = new Result();
    Response response = client.put(entityPath(id), body, INTERNAL_HEADER);
    Instant timestamp = getTimestampFromHeader(response);
    result.setTimestamp(timestamp);
    if (response.getStatus() == STATUS_CREATED) {
      result.setStatus(Status.OK);
    } else {
      result = error();
    }
    return result;
  }

  private static Result remoteDelete(String id, HttpClient client)
      throws InterruptedException, HttpException, PoolException, IOException {
    Result result = new Result();
    Response response = client.delete(entityPath(id), INTERNAL_HEADER);
    Instant timestamp = getTimestampFromHeader(response);
    result.setTimestamp(timestamp);
    if (response.getStatus() == STATUS_ACCEPTED) {
      result.setStatus(Status.OK);
    } else {
      result = error();
    }
    return result;
  }

  static boolean isInternal(Request request) {
    String header = request.getHeader(INTERNAL_HEADER_KEY);
    return header != null && headerValue(header).equals(INTERNAL_HEADER_VALUE);
  }

  private static Instant getTimestampFromHeader(Response response) throws IllegalArgumentException {
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

  static Result mergeResults(Collection<Result> results, int acksRequired) {
    Result result = new Result();
    result.setTimestamp(Instant.MIN);
    result.setStatus(Status.ERROR);
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
  static String headerValue(@NotNull String header) {
    if (!header.startsWith(":")) {
      throw new IllegalArgumentException("Unexpected raw header value: " + header);
    }
    return header.substring(1).trim();
  }
}
