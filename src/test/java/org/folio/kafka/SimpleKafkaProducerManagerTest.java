package org.folio.kafka;

import io.vertx.core.Vertx;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class SimpleKafkaProducerManagerTest {

  @Test
  public void shouldReturnKafkaProduced() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .kafkaHost("localhost")
      .kafkaPort("9092")
      .build();

    SimpleKafkaProducerManager simpleKafkaProducerManager = new SimpleKafkaProducerManager(Vertx.vertx(), kafkaConfig);
    assertNotNull(simpleKafkaProducerManager.createShared("test_event"));
  }
}
