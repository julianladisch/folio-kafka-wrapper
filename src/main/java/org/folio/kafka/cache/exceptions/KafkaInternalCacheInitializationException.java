package org.folio.kafka.cache.exceptions;

public class KafkaInternalCacheInitializationException extends RuntimeException{

  public KafkaInternalCacheInitializationException(String message, Throwable cause) {
    super(message, cause);
  }
}
