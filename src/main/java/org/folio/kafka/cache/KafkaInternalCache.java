package org.folio.kafka.cache;

import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KeyValueIterator;
import lombok.Builder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.cache.exceptions.KafkaInternalCacheInitializationException;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;

/**
 * Cache in Kafka for processing events.
 */
public class KafkaInternalCache {

  public static final String KAFKA_CACHE_NUMBER_OF_PARTITIONS = "kafkacache.topic.number.partitions";
  public static final String KAFKA_CACHE_NUMBER_OF_PARTITIONS_DEFAULT = "1";
  public static final String KAFKA_CACHE_REPLICATION_FACTOR = "kafkacache.topic.replication.factor";
  public static final String KAFKA_CACHE_REPLICATION_FACTOR_DEFAULT = "1";
  public static final String KAFKA_CACHE_RETENTION_MS = "kafkacache.log.retention.ms";
  public static final String KAFKA_CACHE_RETENTION_MS_DEFAULT = "18000000";

  private static final Logger LOGGER = LogManager.getLogger(KafkaInternalCache.class);

  private final KafkaConfig kafkaConfig;
  private Cache<String, String> kafkaCache;

  @Builder
  private KafkaInternalCache(KafkaConfig kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
  }

  /**
   * Initialize cache for Kafka with specific properties in KafkaConfig.
   */
  public void initKafkaCache() {
    Cache<String, String> cache = new KafkaCache<>(
      kafkaConfig.getCacheConfig(),
      Serdes.String(),
      Serdes.String()
    );
    this.kafkaCache = cache;
    createCacheTopic();
    cache.init();
  }

  private void createCacheTopic() {
    String cacheTopic = kafkaConfig.getCacheConfig().getString(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG);
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaUrl());

    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> clusterTopics = adminClient.listTopics().names().get(300000, MILLISECONDS);

      if (clusterTopics.contains(cacheTopic)) {
        LOGGER.info("Kafka cache topic '{}' already exists", cacheTopic);
      } else {
        LOGGER.debug("Creating kafka cache topic '{}'", cacheTopic);
        NewTopic topicRequest = new NewTopic(cacheTopic, getNumberOfPartitions(), getReplicationFactor());
        topicRequest.configs(Map.of(
          CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
          RETENTION_MS_CONFIG, getLogRetentionTime()));

        adminClient.createTopics(Collections.singleton(topicRequest)).all().get(300000, MILLISECONDS);
        LOGGER.info("Kafka cache topic has been successfully created: {}", topicRequest);
      }
    } catch (TimeoutException e) {
      throw new KafkaInternalCacheInitializationException(format("Timed out trying to create Kafka cache topic '%s'", cacheTopic), e);
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        LOGGER.info("Kafka cache topic '{}' has not been created, it's already exists", cacheTopic);
      } else {
        throw new KafkaInternalCacheInitializationException(format("Failed to create Kafka cache topic '%s'", cacheTopic), e);
      }
    }
  }


  /**
   * Put to the cache an element with specific key. Tha value - current time. Example : 2021-03-17T14:58:05.725953.
   * Stored as Strings.
   *
   * @param key - key for the event. Mostly - uuid.
   */
  public void putToCache(String key) {
    String currentTime = LocalDateTime.now().toString();
    kafkaCache.put(key, currentTime);
  }

  /**
   * Check if element in cache exists by specified key.
   *
   * @param key - element`s key.
   * @return true - if cache contains element by this key. false - if not.
   */
  public boolean containsByKey(String key) {
    String value = kafkaCache.get(key);
    return value != null;
  }

  /**
   * Clean Kafka cache from outdated elements.
   * Outdated element - element difference between its value and current time is more or equal to the eventTimeoutHours.
   *
   * @param eventTimeoutHours - timeout for event in hours.
   */
  public void cleanupEvents(int eventTimeoutHours) {
    try {
      List<String> outdatedEvents = new ArrayList<>();

      LocalDateTime currentTime = LocalDateTime.now();
      KeyValueIterator<String, String> events = kafkaCache.all();
      events.forEachRemaining(currentEvent -> {
        if (currentEvent.value != null) {
          LocalDateTime eventTime = LocalDateTime.parse(currentEvent.value);
          long hoursBetween = ChronoUnit.HOURS.between(eventTime, currentTime);
          if (hoursBetween >= eventTimeoutHours) {
            outdatedEvents.add(currentEvent.key);
          }
        }
      });
      if (!outdatedEvents.isEmpty()) {
        LOGGER.info("Clearing cache from outdated events...");
      }

      outdatedEvents.forEach(outdatedEvent -> kafkaCache.remove(outdatedEvent));
    } catch (Exception e) {
      LOGGER.error("Failed to clear outdated elements in kafka cache", e);
    }
  }

  private int getNumberOfPartitions() {
    return Integer.parseInt(System.getenv().getOrDefault(KAFKA_CACHE_NUMBER_OF_PARTITIONS, KAFKA_CACHE_NUMBER_OF_PARTITIONS_DEFAULT));
  }

  private short getReplicationFactor() {
    return Short.parseShort(System.getenv().getOrDefault(KAFKA_CACHE_REPLICATION_FACTOR, KAFKA_CACHE_REPLICATION_FACTOR_DEFAULT));
  }

  private String getLogRetentionTime() {
    return System.getenv().getOrDefault(KAFKA_CACHE_RETENTION_MS, KAFKA_CACHE_RETENTION_MS_DEFAULT);
  }
}
