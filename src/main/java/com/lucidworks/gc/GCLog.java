package com.lucidworks.gc;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encapsulates GC events pulled from parsing a GC Log.
 */
public class GCLog {

  public class UnsupportedGCEventException extends RuntimeException {

    List<String> eventLines;
    int logLineNum;
    int causeLineNum;

    public UnsupportedGCEventException(String msg) {
      this(msg, null, -1, -1);
    }

    public UnsupportedGCEventException(String msg, List<String> eventLines, int logLineNum, int causeLineNum) {
      super(msg);
      this.eventLines = eventLines;
      this.logLineNum = logLineNum;
      this.causeLineNum = causeLineNum;
    }
  }

  public interface GCEventListener {
    void onEventParsed(GCLog gcLog, GCEvent event);
  }

  static {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  public static final String CMS_COLLECTOR = "CMS";
  public static final String G1_COLLECTOR = "G1";

  public static Logger log = Logger.getLogger(GCLog.class);

  static final SimpleDateFormat ISO_8601_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S");

  private static final Pattern sizeInKbPattern = Pattern.compile("^(\\d*)[K|k].*$");
  private static final Pattern pctUsed = Pattern.compile("^(\\d*)% used.*$");
  private static final Pattern spaceSizePattern = Pattern.compile("^total (\\d*)K, used (\\d*)K.*$");

  // 2015-03-05T18:08:35.076+0000
  private static final Pattern timestampPattern =
    Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}\\:\\d{2}\\:\\d{2}\\.\\d{3}[\\+|-]\\d{4}");

  private static final String STOPPED_MSG = "Total time for which application threads were stopped";
  public static final String APP_THREADS_STOPPED = "AppThreadsStopped";
  private static final String CMF = "concurrent mode failure";

  private static final double round(final double d) {
    return Math.round(d * 100d) / 100d;
  }

  protected String javaVers;
  protected String javaGC;
  protected String hostAndPort;
  protected Integer javaPid;
  protected String fileName;
  protected int linesRead = 0;
  protected int parseErrors = 0;
  protected List<GCEvent> events = new ArrayList<GCEvent>();
  protected String javaLine;
  protected String commandLineFlagsLine;
  protected String memoryLine;
  protected GCEventListener eventListener;

  private boolean keepPolling = true;

  public GCLog(String hostAndPort, Integer javaPid, String javaVers, String javaGC, String fileName) {
    this(hostAndPort, javaPid, javaVers, javaGC, fileName, null);
  }

  public GCLog(String hostAndPort, Integer javaPid, String javaVers, String javaGC, String fileName, GCEventListener eventListener) {

    if (javaVers == null || (!javaVers.startsWith("1.8") && !javaVers.startsWith("1.7")))
      throw new IllegalArgumentException("Java version '"+javaVers+
        "' not supported by this parser! Only 1.7.x or 1.8.x supported.");

    if (!CMS_COLLECTOR.equals(javaGC) && !G1_COLLECTOR.equals(javaGC))
      throw new IllegalArgumentException("The '"+javaGC+"' collector is not supported by this parser!");

    if (G1_COLLECTOR.equals(javaGC) && !javaVers.startsWith("1.8"))
      throw new IllegalArgumentException("The "+G1_COLLECTOR+" collector is only supported for Java 1.8+");

    this.javaVers = javaVers;
    this.javaGC = javaGC;
    this.hostAndPort = hostAndPort;
    this.javaPid = javaPid;
    this.fileName = fileName;
    this.eventListener = eventListener;
  }

  public String getJavaVersion() {
    return javaVers;
  }

  public String getJavaGarbageCollectorType() {
    return javaGC;
  }

  public String getHostAndPort() {
    return hostAndPort;
  }

  public Integer getJavaPid() {
    return javaPid;
  }
  
  public String getFileName() {
    return fileName;
  }

  public List<GCEvent> getEvents() {
    return events;
  }

  public int getNumParseErrors() {
    return parseErrors;
  }

  public int getLinesRead() {
    return linesRead;
  }

  public double getLogDurationSecs() {
    if (events.isEmpty())
      return 0d;

    GCEvent first = events.get(0);
    GCEvent last = events.get(events.size()-1);
    long durationMs = last.timestamp.getTime() - first.timestamp.getTime();
    double durationSecs = (double)durationMs/1000d;
    return round(durationSecs);
  }

  public double getApplicationThreadsStoppedSecs() {
    if (events.isEmpty())
      return 0d;

    double totalStoppedSecs = 0d;
    for (GCEvent e : events) {
      if (APP_THREADS_STOPPED.equals(e.type))
        totalStoppedSecs += e.durationSecs;
    }
    return round(totalStoppedSecs);
  }

  public double getApplicationThreadsStoppedPct() {
    double logDuration = getLogDurationSecs();
    return logDuration != 0d ? round(100d * getApplicationThreadsStoppedSecs()/logDuration) : 0d;
  }

  public double getTotalPauseSecs() {
    if (events.isEmpty())
      return 0d;

    double totalPauseSecs = 0d;
    for (GCEvent e : events) {
      if (e.isStopTheWorld())
        totalPauseSecs += e.durationSecs;
    }
    return round(totalPauseSecs);
  }

  public double getFullGCPauseTime() {
    if (events.isEmpty())
      return 0d;

    double totalPauseSecs = 0d;
    for (GCEvent e : events) {
      if (e.isFull)
        totalPauseSecs += e.durationSecs;
    }
    return round(totalPauseSecs);
  }

  public GCEvent getLongestStopTheWorldEvent() {
    GCEvent longest = null;

    for (GCEvent e : events) {
      if (!e.isStopTheWorld())
        continue;

      if (longest != null) {
        if (e.durationSecs > longest.durationSecs)
          longest = e;
      } else {
        longest = e;
      }
    }

    return longest;
  }

  /**
   * Parse a GC Log line-by-line using the provided reader.
   */
  public int parse(final LineNumberReader br) throws Exception {
    final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
    // fire up a producer thread that feeds lines from the reader into the queue the parser uses to read lines from
    final Thread feeder = new Thread() {
      public void run() {
        String line = null;
        try {
          while ((line = br.readLine()) != null) {
            try {
              queue.offer(line, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.interrupted();
              log.error(e);
            }
          }
        } catch (IOException ioExc) {
          log.error(ioExc);
        }
        keepPolling = false; // this tells the queue to stop polling once it sees a null from poll
      }
    };
    feeder.start();
    return parse(queue);
  }

  protected String pollForNextLine(LinkedBlockingQueue<String> queue) {
    String line = null;
    try {
      do {
        line = queue.poll(250, TimeUnit.MILLISECONDS);
      } while (line == null && keepPolling);
    } catch (InterruptedException ie) {
      Thread.interrupted();
      this.keepPolling = false;
    }
    return line;
  }

  /**
   * Parse a GC Log line-by-line by pulling lines from a blocking queue;
   * supports use cases where you want to tail the GC log.
   */
  public int parse(LinkedBlockingQueue<String> queue) throws Exception {

    events.clear();
    linesRead = 0;
    parseErrors = 0;
    javaLine = null;
    commandLineFlagsLine = null;
    memoryLine = null;

    String line = null;
    int lineNum = 0;

    boolean inBlock = false;
    List<String> block = new ArrayList<String>();
    List<String> phaseBlock = new ArrayList<String>();
    int blockStartLine = 0;
    String phaseType = null;

    Date timestamp = null;
    List<String> nextLine = new ArrayList<String>(); // allows us to inject lines when the log is missing line breaks

    Matcher timestampFinder = timestampPattern.matcher("");

    while (true) {

      if (!nextLine.isEmpty()) {
        line = nextLine.remove(0);
      } else {

        line = pollForNextLine(queue);
        if (line == null) {
          // out of lines! do the best we can with the remaining lines in the block
          if (inBlock) {
            // end parse block
            inBlock = false;
            if (block.size() > 0) {
              handleGCEventBlock(lineNum, timestamp, null, block, blockStartLine);
            }
            timestamp = null;
            block.clear();
          }
          break;
        }

        ++lineNum;
      }

      line = line.trim();
      if (line.length() == 0)
        continue;

      if (line.startsWith("CMS: Large ")) {
        continue;
      }

      // this is the most frequent message in the logs
      if (line.indexOf(STOPPED_MSG) != -1 && line.endsWith(" seconds")) {
        parseAppStoppedTimeEvent(line, lineNum);
        continue;
      }

      if (javaLine == null && line.startsWith("Java HotSpot(TM)")) {
        javaLine = line;
        continue;
      }

      if (commandLineFlagsLine == null && line.startsWith("CommandLine flags: ")) {
        commandLineFlagsLine = line.substring("CommandLine flags: ".length());
        continue;
      }

      if (memoryLine == null && line.startsWith("Memory: ")) {
        memoryLine = line.substring("Memory: ".length());
        continue;
      }

      timestampFinder.reset(line);

      // Sometimes, log messages are missing a line-break, so we scan for
      // timestamp pattern and break the line into two lines if needed
      if (timestampFinder.find()) {
        int breakAt = timestampFinder.start();
        if (breakAt > 0) {
          String currLine = line.substring(0,breakAt);
          nextLine.add(line.substring(breakAt));

          if (currLine.startsWith("CMS: abort preclean due to time")) {
            // just skip this line
            line = nextLine.remove(0);
          } else {
            line = currLine;
          }

        } else {
          int at = line.indexOf("[Rescan");
          if (at != -1 && line.endsWith(" secs]")) {
            timestamp = ISO_8601_DATE_FMT.parse(line.substring(0, 28));
            handleGCEventBlock(lineNum, timestamp, null, Collections.singletonList(line), lineNum);
            timestamp = null;
            continue;
          }

          // need to search again
          int startAt = 28;
          timestampFinder.reset(line.substring(startAt));
          if (timestampFinder.find()) {
            breakAt = timestampFinder.start();
            breakAt += startAt;

            String currLine = line.substring(0,breakAt);
            nextLine.add(line.substring(breakAt));
            if (currLine.startsWith("CMS: abort preclean due to time")) {
              // just skip this line
              line = nextLine.remove(0);
            } else {
              line = currLine;
            }
          }
        }
      }

      // sometimes blocks are interleaved into each other ... try to deal with that here ...
      if (line.indexOf("[CMS-") != -1 && line.endsWith("-start]")) {
        int at = line.indexOf("[CMS-");
        phaseType = line.substring(at + 1, line.length() - 7);
        phaseBlock.clear();
        phaseBlock.add(line);
        continue;
      } else if (line.indexOf("[GC ") != -1 && line.endsWith("-start]")) {
        int at = line.indexOf("[GC ");
        phaseType = line.substring(at + 1, line.length() - 7);
        phaseBlock.clear();
        phaseBlock.add(line);
        continue;
      } else {
        if (phaseType != null && line.indexOf("["+phaseType+": ") != -1 && line.indexOf("[Times: user=") != -1 && line.endsWith(" secs]")) {
          // CMS style phase
          phaseBlock.add(line);
          handleGCEventBlock(lineNum, timestamp, phaseType, phaseBlock, blockStartLine);
          phaseBlock.clear();
          phaseType = null;
          continue;
        } else if (phaseType != null && line.indexOf("["+phaseType+", ") != -1 && line.endsWith(" secs]")) {
          // G1 style phase
          phaseBlock.add(line);
          handleGCEventBlock(lineNum, timestamp, phaseType, phaseBlock, blockStartLine);
          phaseBlock.clear();
          phaseType = null;
          continue;
        }
      }

      if (!inBlock) {
        // weird stuff here ... but sometimes the logs just starts with an opening { ???
        if (line.startsWith("{")) {
          inBlock = true;
          blockStartLine = lineNum;
          block.clear();
          timestamp = null;
          block.add(line.substring(1));
        } else {
          // start of new block
          int colonSpace = line.indexOf(": ");
          if (colonSpace != -1) {
            String startOfLine = line.substring(0,colonSpace);
            try {
              timestamp = ISO_8601_DATE_FMT.parse(startOfLine);
            } catch (Exception exc) {
              log.error("Failed to parse '" + startOfLine +
                "' as a timestamp at line " + lineNum + " due to: " + exc + "; line: " + line);
              continue;
            }

            inBlock = true;
            blockStartLine = lineNum;
            block.clear();
            block.add(line);

            // some blocks are single line, such as:
            //2015-03-03T18:14:36.856+0000: 763.717: [GC [1 CMS-initial-mark: 6591678K(12582912K)] \
            // 7291374K(16078208K), 0.0942150 secs] [Times: user=0.08 sys=0.01, real=0.10 secs]
            if (line.indexOf("[GC") != -1 && line.endsWith(" secs]")) {
              // single line block
              inBlock = false;
              handleGCEventBlock(lineNum, timestamp, null, block, blockStartLine);
              timestamp = null;
              block.clear();
            }
          } else {
            log.warn("skipping line " + lineNum +
              " because we're not in an event block and this line doesn't look like the start of a new event, line: " + line);
            continue;
          }
        }
      } else {
        // we're in an event block here
        if (line.startsWith("}")) {
          block.add(line);

          // peek at the next line as it might be part of this block
          line = pollForNextLine(queue);
          if (line != null) {
            ++lineNum;

            timestampFinder.reset(line);

            if (line.startsWith("{") || timestampFinder.find()) {
              // next line is actually the start of a new block
              nextLine.add(line);
            } else {
              // block continued to next line
              block.add(line);
            }
          }

          // end parse block
          inBlock = false;
          if (block.size() > 0) {
            handleGCEventBlock(lineNum, timestamp, null, block, blockStartLine);
          }
          timestamp = null;
          block.clear();
        } else {
          block.add(line);
        }
      }
    }

    this.linesRead = lineNum;

    return lineNum;
  }

  protected void parseAppStoppedTimeEvent(String line, int lineNum) {
    try {
      String toFind = "stopped: ";
      int at = line.indexOf(toFind);
      String tmp = line.substring(at+toFind.length());

      at = tmp.indexOf(", Stopping threads took"); // Java 8
      if (at != -1)
        tmp = tmp.substring(0,at);

      at = tmp.indexOf(" seconds");

      GCEvent event = new GCEvent();
      event.lineNum = lineNum;
      event.timestamp = ISO_8601_DATE_FMT.parse(line.substring(0,28));
      event.type = APP_THREADS_STOPPED;
      event.durationSecs = Double.parseDouble(tmp.substring(0,at).trim());

      events.add(event);

      if (eventListener != null)
        eventListener.onEventParsed(this, event);

    } catch (Exception exc) {
      log.error("Failed to parse application stopped time event '"+
        line+"' at line "+lineNum+" in "+fileName+" due to: "+exc, exc);
      ++parseErrors;
    }
  }

  protected void handleGCEventBlock(int lineNum, Date timestamp, String cmsPhaseType, List<String> block, int blockStartLine) {
    //System.out.println("\n\n"+lineNum+": "+block2str(block)+"\n\n");
    GCEvent event = null;
    try {
      event = parseGCEvent(timestamp, cmsPhaseType, block, blockStartLine);

      // bug checking!!!
      if (event == null)
        throw new IllegalStateException(
          "NULL event from file "+fileName+" at line "+lineNum+"! type is null! block:\n"+block2str(block));

      if (event.type == null)
        throw new IllegalStateException(event+
          " is invalid from file "+fileName+" at line "+lineNum+"! type is null! block:\n"+block2str(block));

      if (event.timestamp == null)
        throw new IllegalStateException(event+
          " is invalid from file "+fileName+" at line "+lineNum+"! timestamp is null! block:\n"+block2str(block));

      if (event.durationSecs == -1d)
        throw new IllegalStateException(event+
          " is invalid from file "+fileName+" at line "+lineNum+"! durationSecs is -1! block:\n"+block2str(block));

      // yay! the event is good
      events.add(event);

      if (eventListener != null)
        eventListener.onEventParsed(this, event);

    } catch (Exception ugc) {
      // don't fail if one event is bad, just log it and keep processing
      log.error(ugc.getMessage(), ugc);
      ++parseErrors;
      System.exit(1);
    }
  }

  private final String block2str(List<String> eventLines) {
    StringBuilder sb = new StringBuilder();
    for (String next : eventLines) sb.append(next).append('\n');
    return sb.toString();
  }

  // 2015-03-03T18:14:36.856+0000: 763.717: [GC [1 CMS-initial-mark: 6591678K(12582912K)] 7291374K(16078208K), 0.0942150 secs] [Times: user=0.08 sys=0.01, real=0.10 secs]
  protected GCEvent parseSingleLineGCEvent(Date timestamp, String line, int lineNum) throws Exception {
    GCEvent event = new GCEvent();

    event.lineNum = lineNum;
    event.timestamp = timestamp;

    int at = line.indexOf(": [GC");
    String tmp = line.substring(at+5);

    if (tmp.indexOf("CMS-initial-mark") != -1) {
      event.type = "CMS-initial-mark";
    } else if (tmp.indexOf("CMS-remark") != -1) {
      event.type = "CMS-remark";
    } else {
      log.warn("Unhandled single line event at "+lineNum+": "+line);
      return null;
    }

    event.durationSecs = parseEventDurationSecs(Collections.singletonList(line), lineNum);
    return event;
  }

  /*
    2015-03-03T18:14:36.950+0000: 763.812: [CMS-concurrent-mark-start]
    2015-03-03T18:14:37.121+0000: 763.982: [CMS-concurrent-mark: 0.171/0.171 secs] [Times: user=1.03 sys=0.00, real=0.17 secs]
   */
  protected GCEvent parseGCPhase(String phaseType, Date timestamp, List<String> lines, int lineNum) throws Exception {
    GCEvent event = new GCEvent();
    event.lineNum = lineNum;
    if (timestamp != null) {
      event.timestamp = timestamp;
    } else {
      String toFind = "[Times: user=";
      int foundAtLine = scanFor(toFind, lines, 0);
      if (foundAtLine == -1) {
        // G1 style
        toFind = "";
      }

      if (foundAtLine != -1) {
        String line = lines.get(foundAtLine);
        String ts = line.substring(0,28);
        try {
          event.timestamp = ISO_8601_DATE_FMT.parse(ts);
        } catch (Exception exc) {
          throw new UnsupportedGCEventException("Failed to parse '"+ts+
            "' as event timestamp at line "+(lineNum+foundAtLine)+" due to: "+exc, lines, lineNum, foundAtLine);
        }
      }
    }

    if (event.timestamp == null)
      throw new UnsupportedGCEventException("Can't determine timestamp for block starting at line "+
        lineNum+":\n"+block2str(lines), lines, lineNum, 0);

    event.type = phaseType;
    event.durationSecs = parseEventDurationSecs(lines, 0);
    return event;
  }

  protected GCEvent parseGCEvent(Date timestamp, String phaseType, List<String> eventLines, int lineNum) throws Exception {
    int numLines = eventLines.size();

    if (numLines == 1) {
      return parseSingleLineGCEvent(timestamp, eventLines.get(0), lineNum);
    }

    if (phaseType != null) {
      return parseGCPhase(phaseType, timestamp, eventLines, lineNum);
    }

    // if we get here, then there's memory info about each generation (yound and old)

    GCGenEvent event = new GCGenEvent();
    event.lineNum = lineNum;
    if (timestamp != null)
      event.timestamp = timestamp;

    // look for either a GC or a Full GC here
    int gcEventAtLine = -1;
    int eventTypeAtLine = -1; // keep track of this so we can use it below

    if (G1_COLLECTOR.equals(javaGC)) {
      gcEventAtLine = parseGCEvent(event, ": [GC pause", lineNum, eventLines);
      if (gcEventAtLine == -1)
        throw new UnsupportedGCEventException("Block starting at line "+
          lineNum+" doesn't appear to be a GC event:\n"+block2str(eventLines), eventLines, lineNum, 0);

      eventTypeAtLine = gcEventAtLine;
      event.type = "G1";

    } else if (CMS_COLLECTOR.equals(javaGC)) {
      gcEventAtLine = parseGCEvent(event, ": [GC", lineNum, eventLines);
      if (gcEventAtLine == -1)
        gcEventAtLine = parseGCEvent(event, ": [Full GC", lineNum, eventLines);

      if (gcEventAtLine == -1)
        throw new UnsupportedGCEventException("Block starting at line "+
          lineNum+" doesn't appear to be a GC event:\n"+block2str(eventLines), eventLines, lineNum, 0);

      // now scan for the event type
      for (int n=gcEventAtLine; n < numLines; n++) {
        String line = eventLines.get(n);
        int typeAt = line.indexOf("[ParNew");
        if (typeAt != -1) {
          event.type = "ParNew";
        } else {
          typeAt = line.indexOf("[CMS");
          if (typeAt != -1) {
            event.type = event.isFull ? "Full GC" : "CMS";
          }
        }

        if (event.type != null) {
          eventTypeAtLine = n;
          break;
        } // else keep scanning for the type
      }
    }

    if (eventTypeAtLine == -1) {
      throw new UnsupportedGCEventException("Couldn't parse event type from block starting at line "+
        lineNum+":\n"+block2str(eventLines), eventLines, lineNum, 0);
    }

    // scan for the event invocation count
    String toFind = "Heap after GC invocations=";
    int foundAtLine = scanFor(toFind, eventLines, 0);
    if (foundAtLine != -1) {
      String line = eventLines.get(foundAtLine);
      int at = line.indexOf(toFind);
      String tmp = line.substring(at+toFind.length());
      int nextSpaceAt = tmp.indexOf(' ');
      if (nextSpaceAt == -1)
        throw new UnsupportedGCEventException("Couldn't parse event invocation count from "+
          tmp+" at line "+(lineNum+foundAtLine), eventLines, lineNum, foundAtLine);

      event.invocation = Integer.parseInt(tmp.substring(0,nextSpaceAt));
    }

    event.durationSecs = parseEventDurationSecs(eventLines, lineNum);
    parseSpaceSizes(event, eventLines, eventTypeAtLine);

    event.edenSpace = parseNewGenSpaceInfo("eden", eventLines, eventTypeAtLine, lineNum);
    event.fromSpace = parseNewGenSpaceInfo("from", eventLines, eventTypeAtLine, lineNum);

    // total hack but treat as separate type
    toFind = CMF;
    foundAtLine = scanFor(toFind, eventLines, 0);
    if (foundAtLine != -1) {
      event.type = CMF;
      event.cause = null;
      event.isFull = true;
    }

    return event;
  }

  protected int parseGCEvent(GCEvent event, String toFind, int lineNum, List<String> eventLines) {
    int gcEventAtLine = -1;
    for (int n=0; n < eventLines.size(); n++) {
      String line = eventLines.get(n);
      int at = line.indexOf(toFind);
      if (at != -1) {
        event.isFull = toFind.indexOf("Full") != -1;

        gcEventAtLine = n;
        // parse out the timestamp if we don't have it already
        if (event.timestamp == null) {
          event.timestamp = parseTimestamp(line, at, lineNum, eventLines, n);
        }
        // event cause
        if (event.cause == null) {
          int offset = at+toFind.length();
          String tmp = line.substring(offset).trim();
          if (tmp.startsWith("(")) {
            int closeParen = tmp.indexOf(")");
            if (closeParen != -1) {
              event.cause = tmp.substring(1,closeParen);
            }
          }
        }
      }
    }
    return gcEventAtLine;
  }

  protected Date parseTimestamp(String line, int at, int lineNum, List<String> eventLines, int n) {
    Date timestamp = null;
    String ts = null;
    String tmp = line.substring(0, at);
    int lastColonAt = tmp.lastIndexOf(':');
    if (lastColonAt != -1)
      ts = tmp.substring(0, lastColonAt);
    if (ts == null)
      throw new UnsupportedGCEventException("Failed to find event timestamp in '"+line+"' at line: "+(lineNum+n),
        eventLines, lineNum, n);

    try {
      timestamp = ISO_8601_DATE_FMT.parse(ts);
    } catch (Exception exc) {
      throw new UnsupportedGCEventException("Failed to parse '"+ts+
        "' as event timestamp at line "+(lineNum+n)+" due to: "+exc, eventLines, lineNum, n);
    }
    return timestamp;
  }

  protected double parseEventDurationSecs(List<String> eventLines, int lineNum) {
    double durationSecs = -1d;
    String toFind = "[Times: user=";
    int foundAtLine = scanFor(toFind, eventLines, 0);
    if (foundAtLine != -1) {
      String line = eventLines.get(foundAtLine);
      int at = line.indexOf(toFind);
      String tmp = line.substring(at+toFind.length());
      String label = " real=";
      int findAt = tmp.indexOf(label);
      if (findAt == -1)
        throw new UnsupportedGCEventException(
          "Couldn't parse event pause duration from "+tmp+" at line "+(lineNum+foundAtLine),
          eventLines, lineNum, foundAtLine);

      String realSecs = tmp.substring(findAt+label.length());
      int nextSpaceAt = realSecs.indexOf(' ');
      if (nextSpaceAt == -1)
        throw new UnsupportedGCEventException(
          "Couldn't parse event pause duration from "+realSecs+" at line "+(lineNum+foundAtLine),
          eventLines, lineNum, foundAtLine);

      durationSecs = Double.parseDouble(realSecs.substring(0, nextSpaceAt));
    }
    return durationSecs;
  }

  protected int scanFor(String toFind, List<String> eventLines) {
    return scanFor(toFind, eventLines, 0);
  }

  protected int scanFor(String toFind, List<String> eventLines, int startScanningAtLine) {
    for (int n=startScanningAtLine; n < eventLines.size(); n++) {
      String line = eventLines.get(n);
      int at = line.indexOf(toFind);
      if (at != -1)
        return n;
    }
    return -1;
  }

  protected void parseSpaceSizes(GCGenEvent event, List<String> eventLines, int eventTypeAtLine) {
    int numLines = eventLines.size();
    for (int n=0; n < numLines; n++) {
      String line = eventLines.get(n);
      int at = line.indexOf("Heap before GC");
      if (at != -1) {
        int[] tmp = parseSpaceSizeInfo("par new generation ", eventLines, n + 1);
        if (tmp != null) {
          event.newGenSpace = new Space("young", tmp[0], tmp[1]);
        }
        tmp = parseSpaceSizeInfo("concurrent mark-sweep generation ", eventLines, n + 1);
        if (tmp != null) {
          event.tenuredSpace = new Space("tenured", tmp[0], tmp[1]);
        }
      }
    }

    for (int n=eventTypeAtLine+1; n < numLines; n++) {
      String line = eventLines.get(n);
      int at = line.indexOf("Heap after GC");
      if (at != -1) {
        int[] tmp = parseSpaceSizeInfo("par new generation ", eventLines, n + 1);
        if (tmp != null) {
          if (event.newGenSpace != null)
            event.newGenSpace.usedAfter = tmp[1];
        }
        tmp = parseSpaceSizeInfo("concurrent mark-sweep generation ", eventLines, n+1);
        if (tmp != null) {
          if (event.tenuredSpace != null)
            event.tenuredSpace.usedAfter = tmp[1];
        }
      }
    }
  }

  protected int[] parseSpaceSizeInfo(String toFind, List<String> eventLines, int startScanningAtLine) {
    int[] spaceSizes = null;
    int foundAt = scanFor(toFind, eventLines, startScanningAtLine);
    if (foundAt != -1) {
      String line = eventLines.get(foundAt);
      int at = line.indexOf(toFind);
      String tmp = line.substring(at+toFind.length());
      Matcher m = spaceSizePattern.matcher(tmp.trim());
      if (m.matches()) {
        spaceSizes = new int[2];
        spaceSizes[0] = Integer.parseInt(m.group(1));
        spaceSizes[1] = Integer.parseInt(m.group(2));
      }
    }
    return spaceSizes;
  }

  protected NewGenSpace parseNewGenSpaceInfo(String spaceName, List<String> eventLines, int eventTypeAtLine, int lineNum) {
    NewGenSpace space = null;

    String searchTerm = spaceName + " space ";

    int numLines = eventLines.size();
    for (int n=0; n < numLines; n++) {
      String line = eventLines.get(n);
      int at = line.indexOf("Heap before GC");
      if (at != -1) {
        for (int m=n+1; m < numLines; m++) {
          line = eventLines.get(m);

          int e = line.indexOf(searchTerm);
          if (e != -1) {
            int parseLine = lineNum+m;
            String tmp = line.substring(e+searchTerm.length()).trim();
            int comma = tmp.indexOf(',');
            if (comma == -1)
              throw new UnsupportedGCEventException("Couldn't parse "+searchTerm+"size from "+line+" at line "+parseLine,
                eventLines, lineNum, m);

            space = new NewGenSpace();
            space.sizeKb = parseSizeKb(tmp.substring(0,comma), line, parseLine);
            space.pctUsedBefore = parsePctUsed(tmp.substring(comma+1), line, parseLine);

            break;
          }
        }

        break;
      }
    }

    if (space == null)
      return null; // couldn't find the before info

    for (int n=0; n < numLines; n++) {
      String line = eventLines.get(n);
      int at = line.indexOf("Heap after GC");
      if (at != -1) {
        for (int m=n+1; m < numLines; m++) {
          line = eventLines.get(m);

          int e = line.indexOf(searchTerm);
          if (e != -1) {

            int parseLine = lineNum+m;
            String tmp = line.substring(e+searchTerm.length()).trim();
            int comma = tmp.indexOf(',');
            if (comma == -1)
              throw new UnsupportedGCEventException("Couldn't parse "+searchTerm+"size from "+line+" at line "+parseLine,
                eventLines, lineNum, m);

            space.pctUsedAfter = parsePctUsed(tmp.substring(comma+1), line, parseLine);

            break;
          }
        }
        break;
      }
    }

    return space;
  }

  protected int parseSizeKb(String toMatch, String line, int lineNum) {
    return parseIntPattern(toMatch, sizeInKbPattern, 1, line, lineNum, "size");
  }

  protected int parsePctUsed(String toMatch, String line, int lineNum) {
    return parseIntPattern(toMatch, pctUsed, 1, line, lineNum, "% used");
  }

  protected int parseIntPattern(String toMatch, Pattern pattern, int group, String line, int lineNum, String label) {
    Integer parsed = null;
    String err = "";
    try {
      Matcher m = pattern.matcher(toMatch.trim());
      if (m.matches())
        parsed = new Integer(m.group(group));
    } catch (Exception nfe) {
      err = " due to: "+nfe.toString();
    }

    if (parsed == null)
      throw new UnsupportedGCEventException(
        "Failed to parse '"+toMatch+"' as "+label+" using pattern '"+
          pattern.toString()+"' from '"+line+"' at "+lineNum+err);

    return parsed.intValue();
  }

  public void printSummaryReport(PrintStream out) {
    Map<String,EventCountAndDuration> eventTypes = new TreeMap<String,EventCountAndDuration>();
    for (GCEvent event : events) {
      String type = String.valueOf(event.type);
      if (event.cause != null) {
        type += " ("+event.cause+")";
      }
      EventCountAndDuration stats = eventTypes.get(type);
      if (stats == null) {
        stats = new EventCountAndDuration();
        eventTypes.put(type, stats);
      }
      stats.count.incrementAndGet();
      stats.durationSecs += event.durationSecs;
    }

    if (hostAndPort != null) {
      out.println("Host: "+hostAndPort+", PID: "+javaPid+", File: "+fileName);
    } else {
      out.println("File: "+fileName);
    }

    out.println("Parsed "+events.size()+" GC events from "+linesRead+" lines, parse errors="+parseErrors);
    out.println("Log Duration: "+getLogDurationSecs()+" secs");
    out.println("Application Threads Stopped Time (GC Overhead): "+getApplicationThreadsStoppedSecs()+" secs");
    out.println("Application Threads Stopped (GC Overhead): "+getApplicationThreadsStoppedPct()+"%");
    out.println("Total Pause Time: "+getTotalPauseSecs()+" secs");
    out.println("Full GC Pause Time: "+getFullGCPauseTime()+" secs");
    out.println("GC Events:");
    for (String type : eventTypes.keySet()) {
      EventCountAndDuration stats = eventTypes.get(type);
      out.println("\t"+type+": "+stats.count.get()+", duration: "+round(stats.durationSecs)+" secs");
    }

    GCEvent longestSTWEvent = getLongestStopTheWorldEvent();
    if (longestSTWEvent != null) {
      out.println("Longest Stop-the-World GC Event:\n"+longestSTWEvent);
    } else {
      out.println("Longest Stop-the-World GC Event: NONE");
    }
  }

  // for summary reporting
  private class EventCountAndDuration {
    AtomicInteger count = new AtomicInteger(0);
    double durationSecs = 0d;
  }

  public class GCEvent {
    String type;
    boolean isFull;
    Date timestamp;
    double durationSecs = -1d;
    String cause;
    int lineNum;

    public boolean isStopTheWorld() {
      return isFull || "CMS-initial-mark".equals(type) || "CMS-remark".equals(type) || "ParNew".equals(type);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append("-").append(String.valueOf(type));
      if (isFull) {
        sb.append(" *FULL*");
      } else {
        if (isStopTheWorld()) sb.append(" *STW*");
      }
      if (timestamp != null) {
        sb.append(" @ ").append(ISO_8601_DATE_FMT.format(timestamp));
      }
      if (cause != null) sb.append(", cause: ").append(cause);
      sb.append(", duration: ").append(durationSecs).append(" secs");
      sb.append(", lineNum: ").append(lineNum);
      return sb.toString();
    }
  }

  public class GCGenEvent extends GCEvent {
    int invocation;
    Space newGenSpace;
    Space tenuredSpace;
    NewGenSpace edenSpace;
    NewGenSpace fromSpace;

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(" #").append(invocation);

      if (tenuredSpace != null) {
        sb.append(", ").append(String.valueOf(tenuredSpace));
        sb.append(", ").append(String.valueOf(newGenSpace));
        sb.append(", eden: ").append(String.valueOf(edenSpace));
        sb.append(", from: ").append(String.valueOf(fromSpace));
      }
      return sb.toString();
    }
  }

  public class Space {
    String name;
    int totalKb;
    int usedBefore;
    int usedAfter;

    public Space(String name, int totalKb, int usedBefore) {
      this.name = name;
      this.totalKb = totalKb;
      this.usedBefore = usedBefore;
    }

    public double getReductionPct() {
      double reduction = 100d * (usedBefore > 0 ? (double)(usedBefore - usedAfter)/usedBefore : 0d);
      return (reduction != 0f) ? -1d * Math.round(reduction * 100d) / 100d : 0d;
    }

    public double getUsedPct() {
      double usedRatio = 100d * (totalKb > 0 ? (double)usedAfter/totalKb : 0d);
      return (usedRatio != 0d) ? Math.round(usedRatio * 100d) / 100d : 0d;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(name).append(" (").append(totalKb).append("K) used: ");
      sb.append(getUsedPct()).append("% delta: ");
      sb.append(usedBefore).append("K -> ").append(usedAfter);
      sb.append("K (").append(getReductionPct()).append("%)");
      return sb.toString();
    }
  }

  public class NewGenSpace {
    int sizeKb;
    int pctUsedBefore;
    int pctUsedAfter;

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(sizeKb);
      sb.append(" delta: ").append(pctUsedBefore);
      sb.append("% -> ").append(pctUsedAfter).append("%");
      return sb.toString();
    }
  }
}
