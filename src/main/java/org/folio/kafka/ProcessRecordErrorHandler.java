package org.folio.kafka;

import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

/**
 * This error handler executed in cases for failed futures.
 * The basic algorithm of work, that it sends DI_ERROR event, mod-source-record-manager accepts these events and stops progress in order to finish import
 * with status 'Completed with errors' and showing error messages instead of hanging progress bar.
 */
public interface ProcessRecordErrorHandler<K, V> {

  /**
   * This method should send DI_ERROR events to make errors visible, to properly track progress bar and eliminate its hang.
   * It should provide RECORD_ID header that is used for messages deduplication in progress bar logic.
   *
   * @param cause  - the cause of the error that prevented successful record processing
   * @param record - the record used for processing
   */
  void handle(Throwable cause, KafkaConsumerRecord<K, V> record);
}
