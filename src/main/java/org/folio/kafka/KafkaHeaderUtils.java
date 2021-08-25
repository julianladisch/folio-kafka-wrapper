package org.folio.kafka;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaHeaderUtils {
  private KafkaHeaderUtils() {
    super();
  }

  public static <K, V> List<KafkaHeader> kafkaHeadersFromMap(Map<K, V> headers) {
    return headers
      .entrySet()
      .stream()
      .map(e -> KafkaHeader.header(String.valueOf(e.getKey()), String.valueOf(e.getValue())))
      .collect(Collectors.toList());
  }

  public static List<KafkaHeader> kafkaHeadersFromMultiMap(MultiMap headers) {
    return headers
      .entries()
      .stream()
      .map(e -> KafkaHeader.header(e.getKey(), e.getValue()))
      .collect(Collectors.toList());
  }

  public static Map<String, String> kafkaHeadersToMap(List<KafkaHeader> headers) {
    return headers
      .stream()
      .collect(Collectors.groupingBy(KafkaHeader::key,
        Collectors.reducing(StringUtils.EMPTY,
          header -> {
            Buffer value = header.value();
            return value == null ? "" : value.toString();
          },
          (a, b) -> StringUtils.isNotBlank(a) ? a : b)));
  }

}
