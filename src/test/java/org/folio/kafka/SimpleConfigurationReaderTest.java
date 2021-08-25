package org.folio.kafka;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SimpleConfigurationReaderTest {

  @Test
  public void shouldReadValueFromSystemProperty() {
    String expectedValue = "testValue";
    System.setProperty("test.props", expectedValue);
    String actualValue = SimpleConfigurationReader.getValue("test.props", null);
    Assert.assertEquals(expectedValue, actualValue);
  }

  @Test
  public void shouldReturnDefaultValueIfNoSysPropertyOrEnvVariable() {
    String defaultValue = "testValue";
    String actualValue = SimpleConfigurationReader.getValue("test.props", defaultValue);
    Assert.assertEquals(defaultValue, actualValue);
  }

  @Test
  public void shouldReadValueFromSystemPropertyBySecondKey() {
    String expectedValue = "testValue";
    System.setProperty("test.props2", expectedValue);
    String actualValue = SimpleConfigurationReader.getValue(List.of("test.props1", "test.props2"), null);
    Assert.assertEquals(expectedValue, actualValue);
  }

  @Test
  public void shouldReturnDefaultValueIfNoValueForSpecifiedKeys() {
    String defaultValue = "testValue";
    String actualValue = SimpleConfigurationReader.getValue(List.of("test.props1", "test.props2"), defaultValue);
    Assert.assertEquals(defaultValue, actualValue);
  }
}
