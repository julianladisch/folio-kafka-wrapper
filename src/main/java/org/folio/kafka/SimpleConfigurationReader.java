package org.folio.kafka;


import java.util.List;

public class SimpleConfigurationReader {
  private SimpleConfigurationReader() {
    super();
  }

  public static String getValue(String key, String defValue) {
    String value = System.getProperty(key, System.getenv(key));
    return value != null ? value : defValue;
  }

  public static String getValue(List<String> keys, String defValue) {
    for (String key : keys) {
      String value = System.getProperty(key, System.getenv(key));
      if (value != null) {
        return value;
      }
    }
    return defValue;
  }
}
