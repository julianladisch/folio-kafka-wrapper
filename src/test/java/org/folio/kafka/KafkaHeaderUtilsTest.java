package org.folio.kafka;

import io.vertx.core.MultiMap;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.hasItem;
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

  @Test
  public void shouldConvertMapToKafkaHeaders() {
    Map<String, String> headers = Map.of(
      "x-okapi-tenant", "diku",
      "x-okapi-user-id", UUID.randomUUID().toString());

    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMap(headers);

    assertEquals(2, kafkaHeaders.size());
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      KafkaHeader header = getKafkaHeader(entry.getKey(), kafkaHeaders);
      assertNotNull(header);
      assertEquals(header.value().toString(), entry.getValue());
    }
  }

  @Test
  public void shouldConvertKafkaHeadersToMap() {
    List<KafkaHeader> kafkaHeaders = List.of(
      KafkaHeader.header("x-okapi-tenant", "diku"),
      KafkaHeader.header("x-okapi-user-id", UUID.randomUUID().toString()));

    Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders);

    assertEquals(2, headersMap.size());
    for (KafkaHeader kafkaHeader : kafkaHeaders) {
      assertNotNull(headersMap.get(kafkaHeader.key()));
      assertEquals(headersMap.get(kafkaHeader.key()), kafkaHeader.value().toString());
    }
  }

  private KafkaHeader getKafkaHeader(String headerName, List<KafkaHeader> kafkaHeaders) {
    return kafkaHeaders.stream()
      .filter(h -> h.key().equals(headerName))
      .findFirst()
      .orElse(null);
  }
}
