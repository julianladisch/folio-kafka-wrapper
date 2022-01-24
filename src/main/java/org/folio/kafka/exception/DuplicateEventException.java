package org.folio.kafka.exception;

public class DuplicateEventException extends RuntimeException {
  public DuplicateEventException(String message) {
    super(message);
  }
}
