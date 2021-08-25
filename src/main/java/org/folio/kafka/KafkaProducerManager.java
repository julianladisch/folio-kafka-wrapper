package org.folio.kafka;

import io.vertx.kafka.client.producer.KafkaProducer;

public interface KafkaProducerManager {
  <K, V> KafkaProducer<K, V> createShared(String name);
}
