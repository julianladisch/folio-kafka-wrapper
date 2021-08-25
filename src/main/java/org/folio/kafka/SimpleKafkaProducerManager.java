package org.folio.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;

public class SimpleKafkaProducerManager implements KafkaProducerManager {
  private static final String PRODUCER_SUFFIX = "_Producer";

  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;

  public SimpleKafkaProducerManager(Vertx vertx, KafkaConfig kafkaConfig) {
    super();
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public <K, V> KafkaProducer<K, V> createShared(String producerName) {
    return KafkaProducer.createShared(vertx, producerName + PRODUCER_SUFFIX, kafkaConfig.getProducerProps());
  }
}
