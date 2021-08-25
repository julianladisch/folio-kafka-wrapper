package org.folio.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static org.folio.kafka.KafkaConfig.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

@RunWith(VertxUnitRunner.class)
public class KafkaConsumerWrapperTest {

  private static final String EVENT_TYPE = "test_topic";
  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID = "diku";
  private static final String MODULE_NAME = "test_module";

  @ClassRule
  public static EmbeddedKafkaCluster kafkaCluster = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
  private static KafkaConfig kafkaConfig;

  private Vertx vertx = Vertx.vertx();

  @BeforeClass
  public static void setUpClass() {
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .build();
  }

  @Test
  public void shouldResumeConsumerAndPollRecordAfterConsumerWasPaused(TestContext testContext) {
    Async async = testContext.async();
    int loadLimit = 5;
    int recordsAmountToSend = 7;
    String expectedLastRecordKey = String.valueOf(recordsAmountToSend);
    System.setProperty(KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG, "2");

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), EVENT_TYPE);
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    String topicName = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, EVENT_TYPE);
    List<Promise<String>> promises = new ArrayList<>();
    AtomicInteger recordCounter = new AtomicInteger(0);

    Future<Void> startFuture = kafkaConsumerWrapper.start(record -> {
      if (recordCounter.incrementAndGet() <= loadLimit) {
        // returns uncompleted futures to keep records in progress and trigger consumer pause
        Promise<String> promise = Promise.promise();
        promises.add(promise);
        return promise.future();
      } else if (recordCounter.get() == loadLimit + 1) {
        // complete previously postponed records to resume consumer
        promises.forEach(p -> p.complete(null));
        return Future.succeededFuture(record.key());
      } else {
        testContext.assertEquals(expectedLastRecordKey, record.key());
        async.complete();
        return Future.succeededFuture(record.key());
      }
    }, MODULE_NAME);

    startFuture.onComplete(v -> {
      for (int i = 1; i <= recordsAmountToSend; i++) {
        sendRecord(String.valueOf(i), format("test_payload-%s", i), topicName, testContext);
      }
    });
  }

  private void sendRecord(String key, String recordPayload, String topicName, TestContext testContext) {
    try {
      KeyValue<String, String> kafkaRecord = new KeyValue<>(String.valueOf(key), recordPayload);
      SendKeyValues<String, String> request = SendKeyValues.to(topicName, Collections.singletonList(kafkaRecord))
        .useDefaults();

      kafkaCluster.send(request);
    } catch (InterruptedException e) {
      testContext.fail(e);
    }
  }

}
