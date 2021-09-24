package org.folio.kafka;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
      .filter(distinctByKey(KafkaHeader::key))
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

  /**
   * Retrieve distinct value by key
   *
   * @param keyExtractor function to get a key
   * @return returns a predicate that maintains state about what it's seen previously, and that returns whether the given element was seen for the first time
   */
  private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
    Set<Object> seen = new HashSet<>();
    return t -> seen.add(keyExtractor.apply(t));
  }

}
