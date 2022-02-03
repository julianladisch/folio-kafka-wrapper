package org.folio.kafka;

import io.kcache.KafkaCacheConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.kcache.KafkaCacheConfig.KAFKACACHE_TOPIC_REQUIRE_COMPACT_CONFIG;

@Getter
@Builder
@ToString
public class KafkaConfig {
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG = "kafka.consumer.auto.offset.reset";
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG_DEFAULT = "earliest";

  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG = "kafka.consumer.metadata.max.age.ms";
  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG_DEFAULT = "30000";

  public static final String KAFKA_NUMBER_OF_PARTITIONS = "kafka.number_of_partitions";
  public static final String KAFKA_NUMBER_OF_PARTITIONS_DEFAULT = "10";

  public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG = "kafka.consumer.max.poll.records";
  public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG_DEFAULT = "100";

  public static final String KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG = "kafka.consumer.max.poll.interval.ms";
  public static final String KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG_DEFAULT = "300000";

  public static final String KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG = "kafka.producer.compression.type";
  public static final String KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG_DEFAULT = "gzip";

  public static final String KAFKA_SECURITY_PROTOCOL_CONFIG = "security.protocol";
  public static final String KAFKA_SECURITY_PROTOCOL_DEFAULT = "PLAINTEXT";

  public static final String KAFKA_SSL_PROTOCOL_CONFIG = "ssl.protocol";
  public static final String KAFKA_SSL_PROTOCOL_DEFAULT = "TLSv1.2";

  public static final String KAFKA_SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";

  public static final String KAFKA_SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";

  public static final String KAFKA_SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";

  public static final String KAFKA_SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
  public static final String KAFKA_SSL_TRUSTSTORE_TYPE_DEFAULT = "JKS";

  public static final String KAFKA_SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";

  public static final String KAFKA_SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";

  public static final String KAFKA_SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
  public static final String KAFKA_SSL_KEYSTORE_TYPE_DEFAULT = "JKS";

  public static final String KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = "ssl.endpoint.identification.algorithm";

  private static final String KAFKA_CACHE_TOPIC_PROPERTY = "kafkacache.topic";
  private static final String KAFKA_CACHE_TOPIC_PROPERTY_DEFAULT = "events_cache";

  private final String kafkaHost;
  private final String kafkaPort;
  private final String okapiUrl;
  private final int replicationFactor;
  private final String envId;
  private final int maxRequestSize;

  public Map<String, String> getProducerProps() {
    Map<String, String> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG, SpringKafkaProperties.KAFKA_PRODUCER_COMPRESSION_TYPE), KAFKA_PRODUCER_COMPRESSION_TYPE_CONFIG_DEFAULT));

    if (getMaxRequestSize() > 0) {
      producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(getMaxRequestSize()));
    }
    ensureSecurityProps(producerProps);
    return producerProps;
  }

  public Map<String, String> getConsumerProps() {
    Map<String, String> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG, SpringKafkaProperties.KAFKA_CONSUMER_MAX_POLL_RECORDS), KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG_DEFAULT));
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG, SpringKafkaProperties.KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS), KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG_DEFAULT));
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG, SpringKafkaProperties.KAFKA_CONSUMER_AUTO_OFFSET_RESET), KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG_DEFAULT));
    consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG, SpringKafkaProperties.KAFKA_CONSUMER_METADATA_MAX_AGE), KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG_DEFAULT));
    ensureSecurityProps(consumerProps);
    return consumerProps;
  }

  public KafkaCacheConfig getCacheConfig() {
    String topicName = SimpleConfigurationReader.getValue(KAFKA_CACHE_TOPIC_PROPERTY, KAFKA_CACHE_TOPIC_PROPERTY_DEFAULT);
    if (StringUtils.isNotBlank(envId)) {
      topicName = String.format("%s.%s", envId, topicName);
    }

    Properties props = new Properties();
    props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://" + getKafkaUrl()); //It should be as PLAINTEXT, as known issue in Kafka.
    props.put(KAFKA_CACHE_TOPIC_PROPERTY, topicName);
    props.put(KAFKACACHE_TOPIC_REQUIRE_COMPACT_CONFIG, false);
    return new KafkaCacheConfig(props);
  }

  public String getKafkaUrl() {
    return kafkaHost + ":" + kafkaPort;
  }

  public int getNumberOfPartitions() {
    return Integer.parseInt(SimpleConfigurationReader.getValue(KAFKA_NUMBER_OF_PARTITIONS, KAFKA_NUMBER_OF_PARTITIONS_DEFAULT));
  }

  private void ensureSecurityProps(Map<String, String> clientProps) {
    clientProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SECURITY_PROTOCOL_CONFIG, SpringKafkaProperties.KAFKA_SECURITY_PROTOCOL), KAFKA_SECURITY_PROTOCOL_DEFAULT));
    clientProps.put(SslConfigs.SSL_PROTOCOL_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_PROTOCOL_CONFIG, SpringKafkaProperties.KAFKA_SSL_PROTOCOL), KAFKA_SSL_PROTOCOL_DEFAULT));
    clientProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_KEY_PASSWORD_CONFIG, SpringKafkaProperties.KAFKA_SSL_KEY_PASSWORD), null));
    clientProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_TRUSTSTORE_LOCATION_CONFIG, SpringKafkaProperties.KAFKA_SSL_TRUSTSTORE_LOCATION), null));
    clientProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_TRUSTSTORE_PASSWORD_CONFIG, SpringKafkaProperties.KAFKA_SSL_TRUSTSTORE_PASSWORD), null));
    clientProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_TRUSTSTORE_TYPE_CONFIG, SpringKafkaProperties.KAFKA_SSL_TRUSTSTORE_TYPE), KAFKA_SSL_TRUSTSTORE_TYPE_DEFAULT));
    clientProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_KEYSTORE_LOCATION_CONFIG, SpringKafkaProperties.KAFKA_SSL_KEYSTORE_LOCATION), null));
    clientProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_KEYSTORE_PASSWORD_CONFIG, SpringKafkaProperties.KAFKA_SSL_KEYSTORE_PASSWORD), null));
    clientProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_KEYSTORE_TYPE_CONFIG, SpringKafkaProperties.KAFKA_SSL_KEYSTORE_TYPE), KAFKA_SSL_KEYSTORE_TYPE_DEFAULT));
    clientProps.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SimpleConfigurationReader.getValue(
      List.of(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SpringKafkaProperties.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM), null));
  }

}
