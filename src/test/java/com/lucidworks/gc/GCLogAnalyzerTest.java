package com.lucidworks.gc;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.List;

public class GCLogAnalyzerTest {

  private static final double DELTA = 1e-15;

  @Test
  public void test() throws Exception {
    testJava7CMS();
    testJava7CMSWithFullGC();
    testJava8CMS();
    //testJava8G1();
  }

  protected GCLog loadTestGCLog(String[] args) throws Exception {
    GCLogAnalyzer app = new GCLogAnalyzer();
    List<GCLog> gcLogs =
      app.run(GCLogAnalyzer.processCommandLineArgs(GCLogAnalyzer.getOptions(), args));
    assertNotNull(gcLogs);
    assertTrue(gcLogs.size() == 1);
    GCLog gcLog = gcLogs.get(0);
    assertNotNull(gcLog);

    // for debugging
    gcLog.printSummaryReport(System.out);
    System.out.println();

    return gcLog;
  }

  private void testJava7CMSWithFullGC() throws Exception {
    String[] args = new String[] {
      "-log", "src/test/resources/gc_java7_cms_full_causes.log",
      "-javaHostAndPort", "localhost:8983",
      "-javaPid", "12345",
      "-javaVers", "1.7.0_75",
      "-javaGC", "CMS"
    };
    GCLog gcLog = loadTestGCLog(args);

    assertTrue(gcLog.getNumParseErrors() == 0);
    assertTrue(gcLog.getLinesRead() == 2043);

    List<GCLog.GCEvent> events = gcLog.getEvents();
    assertNotNull(events);
    assertTrue(events.size() == 1371);
    assertEquals(gcLog.getLogDurationSecs(), 199.19d, DELTA);

    GCLog.GCEvent longestPauseEvent = gcLog.getLongestStopTheWorldEvent();
    assertNotNull(longestPauseEvent);
    assertEquals(gcLog.getTotalPauseSecs(), 10.14d, DELTA);
    assertEquals(gcLog.getApplicationThreadsStoppedSecs(), 14.93d, DELTA);
    assertEquals(gcLog.getFullGCPauseTime(),  2.84d, DELTA);
  }

  private void testJava7CMS() throws Exception {
    String[] args = new String[] {
      "-log", "src/test/resources/gc_java7_cms.log",
      "-javaHostAndPort", "localhost:8983",
      "-javaPid", "12345",
      "-javaVers", "1.7.0_75",
      "-javaGC", "CMS"
    };
    GCLog gcLog = loadTestGCLog(args);

    assertTrue(gcLog.getNumParseErrors() == 0);
    assertTrue(gcLog.getLinesRead() == 56415);

    List<GCLog.GCEvent> events = gcLog.getEvents();
    assertNotNull(events);
    assertTrue(events.size() == 20934);
    assertEquals(gcLog.getLogDurationSecs(), 6844.81d, DELTA);

    GCLog.GCEvent longestPauseEvent = gcLog.getLongestStopTheWorldEvent();
    assertNotNull(longestPauseEvent);
    assertEquals(gcLog.getTotalPauseSecs(), 211.92d, DELTA);
    assertEquals(gcLog.getApplicationThreadsStoppedSecs(), 134.29d, DELTA);
    assertEquals(gcLog.getFullGCPauseTime(), 0d, DELTA);
  }

  private void testJava8CMS() throws Exception {
    String[] args = new String[] {
      "-log", "src/test/resources/gc_java8_cms.log",
      "-javaHostAndPort", "localhost:8983",
      "-javaPid", "12345",
      "-javaVers", "1.8.0_40",
      "-javaGC", "CMS"
    };
    GCLog gcLog = loadTestGCLog(args);

    assertTrue(gcLog.getNumParseErrors() == 0);
    assertTrue(gcLog.getLinesRead() == 5364); // wc -l src/test/resources/gc_java8_cms.log

    List<GCLog.GCEvent> events = gcLog.getEvents();
    assertNotNull(events);
    assertTrue(events.size() == 4868);
    assertEquals(gcLog.getLogDurationSecs(), 4815.27d, DELTA);

    GCLog.GCEvent longestPauseEvent = gcLog.getLongestStopTheWorldEvent();
    assertNotNull(longestPauseEvent);

    assertEquals(gcLog.getTotalPauseSecs(), 20.23d, DELTA);
    assertEquals(gcLog.getApplicationThreadsStoppedSecs(), 207.45d, DELTA);
    assertEquals(gcLog.getFullGCPauseTime(), 0d, DELTA);
  }

  private void testJava8G1() throws Exception {
    String[] args = new String[] {
      "-log", "src/test/resources/gc_java8_g1.log",
      "-javaHostAndPort", "localhost:8983",
      "-javaPid", "12345",
      "-javaVers", "1.8.0_40",
      "-javaGC", "G1"
    };
    GCLog gcLog = loadTestGCLog(args);

    assertTrue(gcLog.getNumParseErrors() == 0);
    assertTrue(gcLog.getLinesRead() == 3335);

    List<GCLog.GCEvent> events = gcLog.getEvents();
    assertNotNull(events);

    /*
    assertTrue(events.size() == 4868);
    assertEquals(gcLog.getLogDurationSecs(), 4815.27d, DELTA);

    GCLog.GCEvent longestPauseEvent = gcLog.getLongestStopTheWorldEvent();
    assertNotNull(longestPauseEvent);

    assertEquals(gcLog.getTotalPauseSecs(), 20.23d, DELTA);
    assertEquals(gcLog.getApplicationThreadsStoppedSecs(), 207.45d, DELTA);
    assertEquals(gcLog.getFullGCPauseTime(), 0d, DELTA);
    */
  }
}
