package org.folio.kafka;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import lombok.experimental.SuperBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

@SuperBuilder
public class KafkaConsumerWrapper<K, V> extends SimpleKafkaConsumerWrapper<K, V> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final int GLOBAL_SENSOR_NA = -1;

  private static final AtomicInteger indexer = new AtomicInteger();

  private final int id = indexer.getAndIncrement();

  private final AtomicInteger localLoadSensor = new AtomicInteger();

  private final AtomicInteger pauseRequests = new AtomicInteger();

  private final GlobalLoadSensor globalLoadSensor;

  private final BackPressureGauge<Integer, Integer, Integer> backPressureGauge;

  private final int loadLimit;

  private final int loadBottomGreenLine;

  public KafkaConsumerWrapper(Vertx vertx,
                              Context context,
                              KafkaConfig kafkaConfig,
                              SubscriptionDefinition subscriptionDefinition,
                              GlobalLoadSensor globalLoadSensor,
                              ProcessRecordErrorHandler<K, V> processRecordErrorHandler,
                              BackPressureGauge<Integer, Integer, Integer> backPressureGauge,
                              int loadLimit) {
    super(vertx, context, kafkaConfig, subscriptionDefinition, processRecordErrorHandler);

    if (loadLimit < 1) {
      throw new IllegalArgumentException("loadLimit must be greater than 0. Current value is " + loadLimit);
    }

    this.globalLoadSensor = globalLoadSensor;
    this.backPressureGauge = backPressureGauge != null ?
      backPressureGauge :
      (g, l, t) -> l > 0 && l > t; // Just the simplest gauge - if the local load is greater than the threshold and above zero
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  @Override
  public void handle(KafkaConsumerRecord<K, V> record) {
    int currentLoad = pauseIfThresholdExceeds();

    LOGGER.debug("Consumer - id: {} subscriptionPattern: {} a Record has been received. key: {} currentLoad: {} globalLoad: {}",
        id, subscriptionDefinition, record.key(), currentLoad, globalLoadSensor != null ? String.valueOf(globalLoadSensor.current()) : "N/A");
    LOGGER.debug("Starting business completion handler, globalLoadSensor: {}", globalLoadSensor);

    try {
      businessHandler.handle(record).onComplete(businessHandlerCompletionHandler(record));
    } finally {
      resumeIfThresholdAllows();
    }
  }

  private int pauseIfThresholdExceeds() {
    int globalLoad = globalLoadSensor != null ? globalLoadSensor.increment() : GLOBAL_SENSOR_NA;

    int currentLoad = localLoadSensor.incrementAndGet();

    if (backPressureGauge.isThresholdExceeded(globalLoad, currentLoad, loadLimit)) {
      int requestNo = pauseRequests.getAndIncrement();
      LOGGER.debug("Threshold is exceeded, preparing to pause, globalLoad: {}, currentLoad: {}, requestNo: {}", globalLoad, currentLoad, requestNo);
      if (requestNo == 0) {
        pause();
        LOGGER.info("Consumer - id: {} subscriptionPattern: {} kafkaConsumer.pause() requested" + " currentLoad: {}, loadLimit: {}", id, subscriptionDefinition, currentLoad, loadLimit);
      }
    }

    return currentLoad;
  }

  private void resumeIfThresholdAllows() {
    int actualCurrentLoad = localLoadSensor.decrementAndGet();

    int globalLoad = globalLoadSensor != null ? globalLoadSensor.decrement() : GLOBAL_SENSOR_NA;

    if (!backPressureGauge.isThresholdExceeded(globalLoad, actualCurrentLoad, loadBottomGreenLine)) {
      int requestNo = pauseRequests.decrementAndGet();
      LOGGER.debug("Threshold is exceeded, preparing to resume, globalLoad: {}, currentLoad: {}, requestNo: {}", globalLoad, actualCurrentLoad, requestNo);
      if (requestNo == 0) {
        resume();
        LOGGER.info("Consumer - id: {} subscriptionPattern: {} kafkaConsumer.resume() requested currentLoad: {} loadBottomGreenLine: {}", id, subscriptionDefinition, actualCurrentLoad, loadBottomGreenLine);
      }
    }
  }
}
