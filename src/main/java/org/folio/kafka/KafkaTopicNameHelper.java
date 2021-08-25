package org.folio.kafka;

import org.apache.commons.lang3.StringUtils;

import static java.lang.String.join;

public class KafkaTopicNameHelper {
  private static final String DEFAULT_NAMESPACE = "Default";

  private KafkaTopicNameHelper() {
    super();
  }

  public static String formatTopicName(String env, String nameSpace, String tenant, String eventType) {
    return join(".", env, nameSpace, tenant, eventType);
  }

  public static String getEventTypeFromTopicName(String topic) {
    int eventTypeIndex = StringUtils.isBlank(topic) ? -1 : topic.lastIndexOf('.') + 1;
    if (eventTypeIndex > 0) {
      return topic.substring(eventTypeIndex);
    } else {
      throw new RuntimeException("Invalid topic name: " + topic + " - expected env.nameSpace.tenant.eventType");
    }
  }

  public static String formatGroupName(String eventType, String moduleName) {
    return join(".", eventType, moduleName);
  }

  public static String formatSubscriptionPattern(String env, String nameSpace, String eventType) {
    return join("\\.", env, nameSpace, "\\w{1,}", eventType);
  }

  public static String getDefaultNameSpace() {
    return DEFAULT_NAMESPACE;
  }

  public static SubscriptionDefinition createSubscriptionDefinition(String env, String nameSpace, String eventType) {
    return SubscriptionDefinition.builder()
      .eventType(eventType)
      .subscriptionPattern(formatSubscriptionPattern(env, nameSpace, eventType))
      .build();
  }
}
