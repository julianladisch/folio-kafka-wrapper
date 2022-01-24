package org.folio.kafka;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.exception.DuplicateEventException;

import java.util.regex.Pattern;

public class KafkaConsumerWrapper<K, V> implements Handler<KafkaConsumerRecord<K, V>> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final int GLOBAL_SENSOR_NA = -1;

  private final static AtomicInteger indexer = new AtomicInteger();

  private final int id = indexer.getAndIncrement();

  private final AtomicInteger localLoadSensor = new AtomicInteger();

  private final AtomicInteger pauseRequests = new AtomicInteger();

  private final Vertx vertx;

  private final Context context;

  private final KafkaConfig kafkaConfig;

  private final SubscriptionDefinition subscriptionDefinition;

  private final GlobalLoadSensor globalLoadSensor;

  private final ProcessRecordErrorHandler<K, V> processRecordErrorHandler;

  private final BackPressureGauge<Integer, Integer, Integer> backPressureGauge;

  private AsyncRecordHandler<K, V> businessHandler;

  private int loadLimit;

  private int loadBottomGreenLine;

  private KafkaConsumer<K, V> kafkaConsumer;

  public int getLoadLimit() {
    return loadLimit;
  }

  public void setLoadLimit(int loadLimit) {
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  @Builder
  private KafkaConsumerWrapper(Vertx vertx, Context context, KafkaConfig kafkaConfig, SubscriptionDefinition subscriptionDefinition,
                               GlobalLoadSensor globalLoadSensor, ProcessRecordErrorHandler<K, V> processRecordErrorHandler, BackPressureGauge<Integer, Integer, Integer> backPressureGauge, int loadLimit) {
    this.vertx = vertx;
    this.context = context;
    this.kafkaConfig = kafkaConfig;
    this.subscriptionDefinition = subscriptionDefinition;
    this.globalLoadSensor = globalLoadSensor;
    this.processRecordErrorHandler = processRecordErrorHandler;
    this.backPressureGauge = backPressureGauge != null ?
      backPressureGauge :
      (g, l, t) -> l > 0 && l > t; // Just the simplest gauge - if the local load is greater than the threshold and above zero
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  public Future<Void> start(AsyncRecordHandler<K, V> businessHandler, String moduleName) {
    LOGGER.debug("KafkaConsumerWrapper is starting for module: {}", moduleName);

    if (businessHandler == null) {
      String failureMessage = "businessHandler must be provided and can't be null.";
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    if (subscriptionDefinition == null || StringUtils.isBlank(subscriptionDefinition.getSubscriptionPattern())) {
      String failureMessage = "subscriptionPattern can't be null nor empty. " + subscriptionDefinition;
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    if (loadLimit < 1) {
      String failureMessage = "loadLimit must be greater than 0. Current value is " + loadLimit;
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    this.businessHandler = businessHandler;
    Promise<Void> startPromise = Promise.promise();

    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaTopicNameHelper.formatGroupName(subscriptionDefinition.getEventType(), moduleName));

    kafkaConsumer = KafkaConsumer.create(vertx, consumerProps);

    kafkaConsumer.handler(this);
    kafkaConsumer.exceptionHandler(throwable -> LOGGER.error("Error while KafkaConsumerWrapper is working: ", throwable));

    Pattern pattern = Pattern.compile(subscriptionDefinition.getSubscriptionPattern());
    kafkaConsumer.subscribe(pattern, ar -> {
      if (ar.succeeded()) {
        LOGGER.info("Consumer created - id: {} subscriptionPattern: {}", id, subscriptionDefinition);
        startPromise.complete();
      } else {
        LOGGER.error("Consumer creation failed", ar.cause());
        startPromise.fail(ar.cause());
      }
    });

    return startPromise.future();
  }

  public Future<Void> stop() {
    Promise<Void> stopPromise = Promise.promise();
    kafkaConsumer.unsubscribe(uar -> {
        if (uar.succeeded()) {
          LOGGER.info("Consumer unsubscribed - id: {} subscriptionPattern: {}", id, subscriptionDefinition);
        } else {
          LOGGER.error("Consumer was not unsubscribed - id: {} subscriptionPattern: {}", id, subscriptionDefinition, uar.cause());
        }
        kafkaConsumer.close(car -> {
          if (uar.succeeded()) {
            LOGGER.info("Consumer closed - id: {} subscriptionPattern: {}", id, subscriptionDefinition);
          } else {
            LOGGER.error("Consumer was not closed - id: {} subscriptionPattern: {}", id, subscriptionDefinition, car.cause());
          }
          stopPromise.complete();
        });
      }
    );

    return stopPromise.future();
  }

  @Override
  public void handle(KafkaConsumerRecord<K, V> record) {
    int globalLoad = globalLoadSensor != null ? globalLoadSensor.increment() : GLOBAL_SENSOR_NA;

    int currentLoad = localLoadSensor.incrementAndGet();

    if (backPressureGauge.isThresholdExceeded(globalLoad, currentLoad, loadLimit)) {
      int requestNo = pauseRequests.getAndIncrement();
      LOGGER.debug("Threshold is exceeded, preparing to pause, globalLoad: {}, currentLoad: {}, requestNo: {}", globalLoad, currentLoad, requestNo);
      if (requestNo == 0) {
        kafkaConsumer.pause();
        LOGGER.info("Consumer - id: {} subscriptionPattern: {} kafkaConsumer.pause() requested" + " currentLoad: {}, loadLimit: {}", id, subscriptionDefinition, currentLoad, loadLimit);
      }
    }

    LOGGER.debug("Consumer - id: {} subscriptionPattern: {} a Record has been received. key: {} currentLoad: {} globalLoad: {}",
      id, subscriptionDefinition, record.key(), currentLoad, globalLoadSensor != null ? String.valueOf(globalLoadSensor.current()) : "N/A");

    businessHandler.handle(record).onComplete(businessHandlerCompletionHandler(record));

  }

  private Handler<AsyncResult<K>> businessHandlerCompletionHandler(KafkaConsumerRecord<K, V> record) {
    LOGGER.debug("Starting business completion handler, globalLoadSensor: {}", globalLoadSensor);

    return har -> {
      try {
        long offset = record.offset() + 1;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, null);
        offsets.put(topicPartition, offsetAndMetadata);
        LOGGER.debug("Consumer - id: {} subscriptionPattern: {} Committing offset: {}", id, subscriptionDefinition, offset);
        kafkaConsumer.commit(offsets, ar -> {
          if (ar.succeeded()) {
            LOGGER.info("Consumer - id: {} subscriptionPattern: {} Committed offset: {}", id, subscriptionDefinition, offset);
          } else {
            LOGGER.error("Consumer - id: {} subscriptionPattern: {} Error while commit offset: {}", id, subscriptionDefinition, offset, ar.cause());
          }
        });

        if (har.failed()) {
          if (har.cause() instanceof DuplicateEventException) {
            LOGGER.info("Duplicate event for a record - id: {} subscriptionPattern: {} offset: {} has been skipped, logging more info about it in error handler", id, subscriptionDefinition, offset);
          } else {
            LOGGER.error("Error while processing a record - id: {} subscriptionPattern: {} offset: {}", id, subscriptionDefinition, offset, har.cause());
          }
          if (processRecordErrorHandler != null) {
            LOGGER.info("Starting error handler to process failures for a record - id: {} subscriptionPattern: {} offset: {} and send DI_ERROR events",
              id, subscriptionDefinition, offset);
            processRecordErrorHandler.handle(har.cause(), record);
          } else {
            LOGGER.warn("Error handler has not been implemented for subscriptionPattern: {} failures", subscriptionDefinition);
          }
        }
      } finally {
        int actualCurrentLoad = localLoadSensor.decrementAndGet();

        int globalLoad = globalLoadSensor != null ? globalLoadSensor.decrement() : GLOBAL_SENSOR_NA;

        if (!backPressureGauge.isThresholdExceeded(globalLoad, actualCurrentLoad, loadBottomGreenLine)) {
          int requestNo = pauseRequests.decrementAndGet();
          LOGGER.debug("Threshold is exceeded, preparing to resume, globalLoad: {}, currentLoad: {}, requestNo: {}", globalLoad, actualCurrentLoad, requestNo);
          if (requestNo == 0) {
//           synchronized (this) { all this is handled within the same verticle
            kafkaConsumer.resume();
            LOGGER.info("Consumer - id: {} subscriptionPattern: {} kafkaConsumer.resume() requested currentLoad: {} loadBottomGreenLine: {}", id, subscriptionDefinition, actualCurrentLoad, loadBottomGreenLine);
//            }
          }
        }
      }
    };
  }

}
