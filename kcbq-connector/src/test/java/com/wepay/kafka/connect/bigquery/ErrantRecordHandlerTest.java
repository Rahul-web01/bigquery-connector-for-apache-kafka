/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.BigQueryError;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ErrantRecordHandlerTest {

  private final CapturingAppender appender = new CapturingAppender();
  private final Logger handlerLogger = Logger.getLogger(ErrantRecordHandler.class);

  @BeforeEach
  public void addAppender() {
    handlerLogger.addAppender(appender);
  }

  @AfterEach
  public void removeAppender() {
    handlerLogger.removeAppender(appender);
  }

  private static SinkRecord record(String topic, int partition, long offset) {
    return new SinkRecord(topic, partition, null, null, null, "value", offset);
  }

  @Test
  public void shouldLogEachRecordWhenNoReporterIsConfigured() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    Set<SinkRecord> records = new LinkedHashSet<>();
    records.add(record("orders", 0, 10L));
    records.add(record("orders", 2, 42L));

    errantRecordHandler.reportErrantRecords(records, new RuntimeException("boom"));

    List<LoggingEvent> warnings = appender.warnings();
    assertEquals(2, warnings.size());
    String first = warnings.get(0).getRenderedMessage();
    assertTrue(first.contains("topic=orders"), first);
    assertTrue(first.contains("partition=0"), first);
    assertTrue(first.contains("offset=10"), first);
    assertTrue(first.contains("boom"), first);
    String second = warnings.get(1).getRenderedMessage();
    assertTrue(second.contains("partition=2"), second);
    assertTrue(second.contains("offset=42"), second);
  }

  @Test
  public void shouldLogEachRecordFromErrorMapWhenNoReporterIsConfigured() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    Map<SinkRecord, Throwable> rowToError = new LinkedHashMap<>();
    rowToError.put(record("payments", 1, 7L), new Throwable("bad row"));
    rowToError.put(record("payments", 1, 8L), null);

    errantRecordHandler.reportErrantRecords(rowToError);

    List<LoggingEvent> warnings = appender.warnings();
    assertEquals(2, warnings.size());
    String first = warnings.get(0).getRenderedMessage();
    assertTrue(first.contains("topic=payments"), first);
    assertTrue(first.contains("partition=1"), first);
    assertTrue(first.contains("offset=7"), first);
    assertTrue(first.contains("bad row"), first);
    assertTrue(warnings.get(1).getRenderedMessage().contains("offset=8"));
  }

  @Test
  public void shouldSendRecordsToReporterWhenConfigured() {
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(reporter);
    SinkRecord sinkRecord = record("orders", 0, 10L);
    RuntimeException error = new RuntimeException("boom");

    errantRecordHandler.reportErrantRecords(Set.of(sinkRecord), error);

    verify(reporter).report(sinkRecord, error);
    assertTrue(appender.warnings().isEmpty());
  }

  @Test
  public void shouldReturnTrueOnAllowedBigQueryReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    List<BigQueryError> bqErrorList = new ArrayList<>();
    bqErrorList.add(new BigQueryError("invalid", "location", "message", "info"));

    // should allow sending records to dlq for bigquery reason:invalid (present in
    // allowedBigQueryErrorReason list)
    boolean expected = errantRecordHandler.isErrorReasonAllowed(bqErrorList);
    assertTrue(expected);
  }

  @Test
  public void shouldReturnFalseOnNonAllowedReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    List<BigQueryError> bqErrorList = new ArrayList<>();
    bqErrorList.add(new BigQueryError("backendError", "location", "message", "info"));

    // Should not allow sending records to dlq for reason not present in
    // allowedBigQueryErrorReason list
    boolean expected = errantRecordHandler.isErrorReasonAllowed(bqErrorList);
    assertFalse(expected);
  }

  /** Collects log events so tests can check what was logged. */
  private static class CapturingAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }

    List<LoggingEvent> warnings() {
      List<LoggingEvent> warnings = new ArrayList<>();
      for (LoggingEvent event : events) {
        if (event.getLevel().equals(Level.WARN)) {
          warnings.add(event);
        }
      }
      return warnings;
    }

    @Override
    public void close() {}

    @Override
    public boolean requiresLayout() {
      return false;
    }
  }
}
