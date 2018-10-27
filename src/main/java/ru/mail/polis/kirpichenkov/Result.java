package ru.mail.polis.kirpichenkov;

/**
 * Wrapper for the result, retrieved from local storage of from another node
 * @author Pavel Kirpichenkov
 */
public class Result {
  private byte[] body;
  private Status status;
  private long timestamp;

  Result() {
    status = Status.NONE;
  }

  byte[] getBody() {
    return body;
  }

  long getTimestamp() {
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

  void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  enum Status {
    OK,
    ABSENT,
    DELETED,
    NONE
  }
}
