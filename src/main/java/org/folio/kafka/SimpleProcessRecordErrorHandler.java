package org.folio.kafka;

import io.vertx.core.Vertx;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.Builder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class SimpleProcessRecordErrorHandler<K, V> implements ProcessRecordErrorHandler<K, V> {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final int DEFAULT_DELAY_QUANTUM = 250; //milliseconds

  private final int maxResendNumber;

  private final int delayQuantum;

  private final KafkaProducerManager kafkaProducerManager;

  private final Vertx vertx;

  @Builder
  private SimpleProcessRecordErrorHandler(int maxResendNumber, int delayQuantum, KafkaProducerManager kafkaProducerManager, Vertx vertx) {
    super();
    this.maxResendNumber = maxResendNumber;
    this.delayQuantum = (delayQuantum > 0) ? delayQuantum : DEFAULT_DELAY_QUANTUM;
    this.kafkaProducerManager = kafkaProducerManager;
    this.vertx = vertx;
  }

  @Override
  public void handle(Throwable cause, KafkaConsumerRecord<K, V> record) {
    int resendCounter = Integer.parseInt(
      record.headers()
        .stream()
        .filter(h -> RESEND_COUNTER.equalsIgnoreCase(h.key()))
        .map(h -> String.valueOf(h.value()))
        .findFirst()
        .orElse("0"));

    if (resendCounter == maxResendNumber) {
      LOGGER.error("Max resend number reached (" + maxResendNumber + ") for record: " + record);
      return;
    }

    //Let's prepare all beforehand just to release all objects related to record (KafkaConsumerRecord)
    String topic = record.topic();
    String eventType = KafkaTopicNameHelper.getEventTypeFromTopicName(topic);

    KafkaProducerRecord<K, V> producerRecord = KafkaProducerRecord.create(topic, record.key(), record.value());

    //Just remove RESEND_COUNTER header if it is already there
    List<KafkaHeader> headers = record.headers();
    headers.removeIf(kafkaHeader -> RESEND_COUNTER.equalsIgnoreCase(kafkaHeader.key()));

    producerRecord.addHeaders(headers);
    producerRecord.addHeader(RESEND_COUNTER, String.valueOf(++resendCounter));

    int delay = (int) Math.exp(++resendCounter) * delayQuantum;
    //It will be scheduled to run on the current context (verticle)
    vertx.setTimer(delay, id -> {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Record will be resent: {}", producerRecord);
      }
      KafkaProducer<K, V> kafkaProducer = kafkaProducerManager.createShared(eventType);
      kafkaProducer.write(producerRecord, war -> {
        if (war.failed()) {
          LOGGER.error(war.cause());
          kafkaProducer.close();
        }
      });
    });

  }
}
