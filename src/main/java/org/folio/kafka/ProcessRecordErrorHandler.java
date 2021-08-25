package org.folio.kafka;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public interface ProcessRecordErrorHandler<K, V> {
  String RESEND_COUNTER = "on-error-resend-counter";

  /**
   * This method must make a decision on whether this record should be resend or not
   *
   * @param cause  - the cause of the error that prevented successful record processing
   * @param record - the record used for processing
   */
  void handle(Throwable cause, KafkaConsumerRecord<K, V> record);
}
