package ru.mail.polis.kirpichenkov;

import java.time.Instant;

/**
 * Wrapper for the result, retrieved from local storage of from another node
 *
 * @author Pavel Kirpichenkov
 */
public class Result {
  private byte[] body;
  private Status status;
  private Instant timestamp;

  Result() {
    status = Status.ERROR;
  }

  byte[] getBody() {
    return body;
  }

  Instant getTimestamp() {
    return timestamp;
  }

  Status getStatus() {
    return status;
  }

  void setBody(byte[] body) {
    this.body = body;
  }

  void setStatus(Status status) {
    this.status = status;
  }

  void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
  }

  enum Status {
    OK,
    ABSENT,
    DELETED,
    ERROR
  }
}
