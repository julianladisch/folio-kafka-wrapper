package org.folio.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.lang.String.format;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static org.folio.kafka.KafkaConfig.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(VertxUnitRunner.class)
public class KafkaConsumerWrapperTest {

  private static final String EVENT_TYPE = "test_topic";
  private static final String KAFKA_ENV = "test-env";
  private static final String TENANT_ID = "diku";
  private static final String MODULE_NAME = "test_module";

  @Rule
  public EmbeddedKafkaCluster kafkaCluster = provisionWith(EmbeddedKafkaClusterConfig.useDefaults());
  private Vertx vertx = Vertx.vertx();
  private KafkaConfig kafkaConfig;
  private KafkaAdminClient kafkaAdminClient;

  @Before
  public void setUp() {
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .build();

    kafkaAdminClient = KafkaAdminClient.create(vertx, Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaUrl()));
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

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedBusinessHandlerIsNull(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), EVENT_TYPE);
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(null, MODULE_NAME);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSubscriptionDefinitionIsNull(TestContext testContext) {
    Async async = testContext.async();
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .subscriptionDefinition(null)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(record -> Future.succeededFuture(), MODULE_NAME);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedLoadLimitLessThenOne(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), EVENT_TYPE);
    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(0)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> future = kafkaConsumerWrapper.start(record -> Future.succeededFuture(), MODULE_NAME);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnSucceededFutureAndUnsubscribeWhenStopIsCalled(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), EVENT_TYPE);
    String groupId = KafkaTopicNameHelper.formatGroupName(EVENT_TYPE, MODULE_NAME);

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    Future<Void> stopFuture = kafkaConsumerWrapper
      .start(record -> Future.succeededFuture(), MODULE_NAME)
      // wait for joining to group
      .compose(v -> runWithDelay(1000, () -> kafkaAdminClient.describeConsumerGroups(List.of(groupId))))
      .onComplete(ar -> testContext.assertTrue(ar.succeeded()))
      .onSuccess(groups -> testContext.assertEquals(1, groups.get(groupId).getMembers().size()))
      .compose(v -> kafkaConsumerWrapper.stop());

    stopFuture.compose(v -> kafkaAdminClient.describeConsumerGroups(List.of(groupId)))
      .onComplete(ar -> {
        testContext.assertTrue(ar.succeeded());
        testContext.assertEquals(0, ar.result().get(groupId).getMembers().size());
        async.complete();
      });
  }

  @Test
  public void shouldInvokeSpecifiedProcessRecordErrorHandlerWhenAsyncRecordHandlerFails(TestContext testContext) {
    Async async = testContext.async();
    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(KAFKA_ENV, getDefaultNameSpace(), EVENT_TYPE);
    String topicName = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, EVENT_TYPE);
    ProcessRecordErrorHandler<String, String> recordErrorHandler = mock(ProcessRecordErrorHandler.class);

    KafkaConsumerWrapper<String, String> kafkaConsumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(vertx.getOrCreateContext())
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(5)
      .globalLoadSensor(new GlobalLoadSensor())
      .subscriptionDefinition(subscriptionDefinition)
      .processRecordErrorHandler(recordErrorHandler)
      .build();

    kafkaConsumerWrapper
      .start(record -> {
        async.complete();
        return Future.failedFuture("test error msg");
      }, MODULE_NAME)
      .onComplete(v -> sendRecord("1", "test_payload", topicName, testContext));

    async.await();
    verify(recordErrorHandler, after(500)).handle(any(Throwable.class), any(KafkaConsumerRecord.class));
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

  private <T> Future<T> runWithDelay(long delay, Supplier<Future<T>> task) {
    Promise<T> promise = Promise.promise();
    vertx.setTimer(delay, id -> task.get().onComplete(promise));
    return promise.future();
  }

}
