package org.folio.kafka.cache.util;

import org.folio.kafka.cache.KafkaInternalCache;

import io.vertx.core.Vertx;

/**
 * Util for Kafka cache.
 */
public class CacheUtil {

  private CacheUtil() {
  }

  /**
   * Set periodic task (cleanup cache from outdated events) to the vertx with specific params.
   * @param vertx - vertx for periodic.
   * @param kafkaInternalCache - kafka cache.
   * @param periodicTime - time between executing periodic task.
   * @param eventTimeoutHours - timeout for event in hours.
   */
  public static void initCacheCleanupPeriodicTask(Vertx vertx, KafkaInternalCache kafkaInternalCache, long periodicTime, int eventTimeoutHours) {
    vertx.setPeriodic(periodicTime,
      e -> vertx.<Void>executeBlocking(b -> kafkaInternalCache.cleanupEvents(eventTimeoutHours)));
  }
}
