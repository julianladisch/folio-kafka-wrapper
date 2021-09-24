package org.folio.kafka;

import io.vertx.core.MultiMap;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class KafkaHeaderUtilsTest {

  @Test
  public void shouldReturnDistinctValuesInListWhenThereAreDuplicateElements() {
    MultiMap headers = new HeadersMultiMap();
    headers.add("x-okapi-request-method", "POST");
    headers.add("x-okapi-request-method", "POST");
    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(headers);
    assertEquals(1, kafkaHeaders.size());
  }
}
