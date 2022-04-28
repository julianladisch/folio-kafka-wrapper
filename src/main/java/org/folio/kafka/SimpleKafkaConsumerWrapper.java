package org.folio.kafka;

import io.vertx.core.*;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.exception.DuplicateEventException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

@SuperBuilder
public class SimpleKafkaConsumerWrapper<K, V> implements Handler<KafkaConsumerRecord<K, V>>  {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final AtomicInteger indexer = new AtomicInteger();

  private final int id = indexer.getAndIncrement();

  protected Vertx vertx;

  protected Context context;

  protected KafkaConfig kafkaConfig;

  protected SubscriptionDefinition subscriptionDefinition;

  protected ProcessRecordErrorHandler<K, V> processRecordErrorHandler;

  protected AsyncRecordHandler<K, V> businessHandler;

  protected KafkaConsumer<K, V> kafkaConsumer;

  public SimpleKafkaConsumerWrapper(Vertx vertx,
                                    Context context,
                                    KafkaConfig kafkaConfig,
                                    SubscriptionDefinition subscriptionDefinition,
                                    ProcessRecordErrorHandler<K, V> processRecordErrorHandler) {
    if (subscriptionDefinition == null || StringUtils.isBlank(subscriptionDefinition.getSubscriptionPattern())) {
      throw new IllegalArgumentException("subscriptionPattern can't be null nor empty. " + subscriptionDefinition);
    }

    this.vertx = vertx;
    this.context = context;
    this.kafkaConfig = kafkaConfig;
    this.subscriptionDefinition = subscriptionDefinition;
    this.processRecordErrorHandler = processRecordErrorHandler;
  }

  @Override
  public void handle(KafkaConsumerRecord<K, V> record) {
    LOGGER.debug("Consumer - id: {} subscriptionPattern: {} a Record has been received. key: {}. Starting business completion handler.",
      id, subscriptionDefinition, record.key());

    businessHandler.handle(record).onComplete(businessHandlerCompletionHandler(record));
  }

  /**
   * Starts kafka consumer.
   *
   * @param businessHandler the business handler to invoke
   * @param moduleName the module name
   * @return void future
   */
  public Future<Void> start(AsyncRecordHandler<K, V> businessHandler, String moduleName) {
    LOGGER.debug("KafkaConsumerWrapper is starting for module: {}", moduleName);

    if (businessHandler == null) {
      return Future.failedFuture(new IllegalArgumentException("businessHandler must be provided and can't be null."));
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

  /**
   * Stops kafka consumer.
   * This operation includes unsubscribing and closing kafka consumer.
   *
   * @return void future
   */
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

  /**
   * Pauses kafka consumer.
   */
  public void pause() {
    kafkaConsumer.pause();
  }

  /**
   * Resumes kafka consumer.
   */
  public void resume() {
    kafkaConsumer.resume();
  }

  /**
   * Gets usage demand to determine if consumer paused.
   *
   * @return 0 if consumer paused, otherwise any value greater than 0 would mean that consumer working.
   */
  public long demand() {
    return kafkaConsumer.demand();
  }

  /**
   * Handler that invoked when kafka record received.
   *
   * @param record the incoming kafka record
   * @return async result
   */
  protected Handler<AsyncResult<K>> businessHandlerCompletionHandler(KafkaConsumerRecord<K, V> record) {
    return har -> {
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
    };
  }
}
