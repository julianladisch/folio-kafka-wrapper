package org.folio.kafka;

public final class SpringKafkaProperties {

  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "spring.kafka.consumer.auto-offset-reset";

  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE = "spring.kafka.consumer.properties.metadata.max.age.ms";

  public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS = "spring.kafka.consumer.max-poll-records";

  public static final String KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS = "spring.kafka.consumer.properties.max.poll.interval.ms";

  public static final String KAFKA_SECURITY_PROTOCOL = "spring.kafka.security.protocol";

  public static final String KAFKA_SSL_PROTOCOL = "spring.kafka.ssl.protocol";

  public static final String KAFKA_SSL_KEY_PASSWORD = "spring.kafka.ssl.key-password";

  public static final String KAFKA_SSL_TRUSTSTORE_LOCATION = "spring.kafka.ssl.trust-store-location";

  public static final String KAFKA_SSL_TRUSTSTORE_PASSWORD = "spring.kafka.ssl.trust-store-password";

  public static final String KAFKA_SSL_TRUSTSTORE_TYPE = "spring.kafka.ssl.trust-store-type";

  public static final String KAFKA_SSL_KEYSTORE_LOCATION = "spring.kafka.ssl.key-store-location";

  public static final String KAFKA_SSL_KEYSTORE_PASSWORD = "spring.kafka.ssl.key-store-password";

  public static final String KAFKA_SSL_KEYSTORE_TYPE = "spring.kafka.ssl.key-store-type";

  public static final String KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "spring.kafka.properties.ssl.endpoint.identification.algorithm";

  private SpringKafkaProperties() {
    throw new UnsupportedOperationException();
  }
}
