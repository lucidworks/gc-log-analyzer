package com.lucidworks.gc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import org.apache.commons.io.input.Tailer;

/**
 * Command-line utility for analyzing a GC log, and optionally sending GC events into a Solr collection for
 * further analysis using Banana and other Solr analysis tools, such as facets and stats.
 */
public class GCLogAnalyzer {

  static {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  public static Logger log = Logger.getLogger(GCLogAnalyzer.class);

  static Option[] options() {
    return new Option[]{
      OptionBuilder
        .withArgName("HOST:PORT")
        .hasArg()
        .isRequired(true)
        .withDescription("Host and port of system that created the GC log(s) to parse.")
        .create("javaHostAndPort"),
      OptionBuilder
        .withArgName("PID")
        .hasArg()
        .isRequired(true)
        .withDescription("Process ID of the JVM that created the GC log(s) to parse.")
        .create("javaPid"),
      OptionBuilder
        .withArgName("VERS")
        .hasArg()
        .isRequired(true)
        .withDescription("Version of the JVM that created the GC log(s) to parse.")
        .create("javaVers"),
      OptionBuilder
        .withArgName("GC")
        .hasArg()
        .isRequired(false)
        .withDescription("Java garbage collector that created the GC log(s) to parse; e.g. CMS or G1")
        .create("javaGC"),
      OptionBuilder
        .withArgName("PATH")
        .hasArg()
        .isRequired(false)
        .withDescription("Path to a gc log; either flat text or gzipped are supported")
        .create("log"),
      OptionBuilder
        .withArgName("PATH")
        .hasArg()
        .isRequired(false)
        .withDescription("Path to a directory containing gc log files")
        .create("dir"),
      OptionBuilder
        .withArgName("HOST")
        .hasArg()
        .isRequired(false)
        .withDescription("Address of the Zookeeper ensemble for indexing into SolrCloud")
        .create("zkHost"),
      OptionBuilder
        .withArgName("COLLECTION")
        .hasArg()
        .isRequired(false)
        .withDescription("Name of collection to send GC log events to for analysis; no default")
        .create("collection"),
      OptionBuilder
        .isRequired(false)
        .withDescription("Flag to indicate this app should tail the provided log file; only valid if using the -log option")
        .create("tail")
    };
  }

  public static void main(String[] args) throws Exception {
    Options opts = getOptions();
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args!");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(GCLogAnalyzer.class.getName(), opts);
      System.exit(1);
    }
    (new GCLogAnalyzer()).run(processCommandLineArgs(opts, args));
  }

  /**
   * Parses a GC log (-log) or directory containing GC logs (-dir).
   */
  public List<GCLog> run(CommandLine cli) throws Exception {

    // must provide either -dir or -log argument
    if (!cli.hasOption("dir") && !cli.hasOption("log")) {
      System.err.println("Invalid command-line args!");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(GCLogAnalyzer.class.getName(), getOptions());
      System.exit(1);
    }

    String collection = cli.getOptionValue("collection");
    String zkHost = cli.getOptionValue("zkHost");

    List<GCLog> gcLogs = null;
    CloudSolrServer cloudSolrServer = null;
    try {
      if (zkHost != null) {
        log.info("Connecting to SolrCloud cluster: " + zkHost);
        cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.setDefaultCollection(collection);
        cloudSolrServer.connect();
      }

      gcLogs = processGCLogs(cloudSolrServer, cli);
    } finally {
      if (cloudSolrServer != null) {
        try {
          cloudSolrServer.shutdown();
        } catch (Exception ignore) {}
        log.info("Shutdown CloudSolrServer.");
      }
    }
    
    return gcLogs;
  }

  public List<GCLog> processGCLogs(CloudSolrServer cloudSolrServer, CommandLine cli) throws Exception {

    List<GCLog> gcLogs = new ArrayList<GCLog>();
    if (cli.hasOption("dir")) {
      File dir = new File(cli.getOptionValue("dir"));
      if (!dir.isDirectory())
        throw new FileNotFoundException("GC log directory " + dir.getAbsolutePath() + " not found!");

      File[] files = dir.listFiles(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          String nameLc = name.toLowerCase();
          return nameLc.endsWith(".gz") || nameLc.endsWith(".log");
        }
      });

      for (File next : files) {
        GCLog gcLog = parseGCLogFile(cli, next);
        if (cloudSolrServer != null)
          indexGCLog(cloudSolrServer, gcLog);

        gcLogs.add(gcLog);
      }

    } else {
      File logFile = new File(cli.getOptionValue("log"));
      if (!logFile.isFile())
        throw new FileNotFoundException("GC log file " + logFile.getAbsolutePath() + " not found!");

      if (cli.hasOption("tail")) {
        // user has requested us to tail a GC log file
        GCLogTailer tailerListener = new GCLogTailer(cloudSolrServer);
        GCLog gcLog = newGCLogInstance(cli, logFile.getName(), tailerListener);
        Tailer tailer = new Tailer(logFile, tailerListener, 1000);
        Thread thread = new Thread(tailer);
        thread.setDaemon(true);
        thread.start(); // start queuing events

        // app blocks here indefinitely
        gcLog.parse(tailerListener.queue);

      } else {
        GCLog gcLog = parseGCLogFile(cli, logFile);

        if (cloudSolrServer != null)
          indexGCLog(cloudSolrServer, gcLog);

        gcLogs.add(gcLog);
      }

    }

    return gcLogs;
  }

  public class GCLogTailer extends TailerListenerAdapter implements GCLog.GCEventListener {

    CloudSolrServer cloudSolrServer;
    LinkedBlockingQueue<String> queue;

    GCLogTailer(CloudSolrServer cloudSolrServer) {
      super();
      this.cloudSolrServer = cloudSolrServer;
      this.queue = new LinkedBlockingQueue<String>(1000);
    }

    public void handle(String line) {
      try {
        queue.offer(line, 10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.interrupted();
        log.error(e);
      }
    }

    public void onEventParsed(GCLog gcLog, GCLog.GCEvent event) {
      if (cloudSolrServer != null) {
        try {
          sendBatch(cloudSolrServer, Collections.singletonList(toDoc(gcLog, event)), 2, 2);
        } catch (Exception exc) {
          log.error("Failed to index GC event '"+event+"' into Solr due to: "+exc, exc);
        }
      } else {
        log.info("log tailer: "+event);
      }
    }
  }

  public GCLog newGCLogInstance(CommandLine cli, String fileName, GCLog.GCEventListener eventListener) {
    String hostAndPort = cli.getOptionValue("javaHostAndPort");
    int javaPid = Integer.parseInt(cli.getOptionValue("javaPid"));
    String javaVers = cli.getOptionValue("javaVers");
    String javaGC = cli.getOptionValue("javaGC", "CMS");
    return new GCLog(hostAndPort, javaPid, javaVers, javaGC, fileName, eventListener);
  }

  public GCLog parseGCLogFile(CommandLine cli, File logFile) throws Exception {
    log.info("Parsing GC log file: " + logFile.getName());
    GCLog gcLog = newGCLogInstance(cli, logFile.getName(), null);

    // parse the log one-by-one to generate a List of GCEvent objects
    LineNumberReader br = null;
    try {
      InputStreamReader isr = null;
      if (logFile.getName().toLowerCase().endsWith(".gz")) {
        isr = new InputStreamReader(
          new GzipCompressorInputStream(
            new BufferedInputStream(
              new FileInputStream(logFile))), StandardCharsets.UTF_8);
      } else {
        isr = new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8);
      }

      br = new LineNumberReader(isr);
      gcLog.parse(br);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (Exception ignore) {}
      }
    }

    return gcLog;
  }

  public void indexGCLog(CloudSolrServer cloudSolrServer, GCLog gcLog) throws Exception {
    List<GCLog.GCEvent> events = gcLog.getEvents();
    int batchSize = 200;
    List<SolrInputDocument> batch = new ArrayList<SolrInputDocument>(batchSize);
    for (GCLog.GCEvent next : events) {
      batch.add(toDoc(gcLog, next));
      if (batch.size() >= batchSize)
        sendBatch(cloudSolrServer, batch, 2, 2);
    }
    if (!batch.isEmpty())
      sendBatch(cloudSolrServer, batch, 2, 2);
  }

  protected SolrInputDocument toDoc(GCLog gcLog, GCLog.GCEvent event) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();

    String docId = String.format("%s-%d-%d-%s",
      gcLog.getHostAndPort(), gcLog.getJavaPid(), event.timestamp.getTime(), event.type);
    doc.setField("id", docId);
    doc.setField("host_port_s", gcLog.getHostAndPort());
    doc.setField("java_pid_i", gcLog.getJavaPid());
    doc.setField("gc_log_file_s", gcLog.getFileName());
    doc.setField("timestamp_tdt", event.timestamp);
    doc.setField("type_s", "gc");
    doc.setField("java_vers_s", gcLog.getJavaVersion());
    doc.setField("gc_type_s", gcLog.getJavaGarbageCollectorType());
    doc.setField("gc_event_type_s", event.type);
    doc.setField("gc_log_line_i", event.lineNum);
    doc.setField("duration_secs_d", event.durationSecs);
    doc.setField("is_stop_the_world_b", event.isStopTheWorld());
    doc.setField("is_full_gc_b", event.isFull);
    if (event.cause != null)
      doc.setField("cause_s", event.cause);

    if (event instanceof GCLog.GCGenEvent) {
      GCLog.GCGenEvent genEvent = (GCLog.GCGenEvent)event;
      doc.setField("gc_invocation_i", genEvent.invocation);

      if (genEvent.tenuredSpace != null)
        addGenSpaceFields(genEvent.tenuredSpace, doc);

      if (genEvent.newGenSpace != null)
        addGenSpaceFields(genEvent.newGenSpace, doc);

      if (genEvent.edenSpace != null)
        addNewGenSpaceFields("eden", genEvent.edenSpace, doc);

      if (genEvent.fromSpace != null)
        addNewGenSpaceFields("from", genEvent.fromSpace, doc);
    }

    return doc;
  }

  protected void addNewGenSpaceFields(String label, GCLog.NewGenSpace newGenSpace, SolrInputDocument doc) {
    doc.setField(label+"_size_kb_i", newGenSpace.sizeKb);
    doc.setField(label+"_used_before_pct_d", newGenSpace.pctUsedBefore);
    doc.setField(label+"_used_after_pct_d", newGenSpace.pctUsedAfter);
  }

  protected void addGenSpaceFields(GCLog.Space space, SolrInputDocument doc) {
    doc.setField(space.name+"_size_kb_i", space.totalKb);
    doc.setField(space.name+"_used_before_kb_i", space.usedBefore);
    doc.setField(space.name+"_used_after_kb_i", space.usedAfter);
    doc.setField(space.name+"_used_pct_d", space.getUsedPct());
    doc.setField(space.name+"_used_reduced_d", space.getReductionPct());
  }

  protected int sendBatch(CloudSolrServer cloudSolrServer, List<SolrInputDocument> batch, int waitBeforeRetry, int maxRetries) throws Exception {
    int sent = 0;
    try {
      UpdateRequest updateRequest = new UpdateRequest();
      ModifiableSolrParams params = updateRequest.getParams();
      if (params == null) {
        params = new ModifiableSolrParams();
        updateRequest.setParams(params);
      }
      updateRequest.add(batch);
      cloudSolrServer.request(updateRequest);
      sent = batch.size();
    } catch (Exception exc) {

      Throwable rootCause = SolrException.getRootCause(exc);
      boolean wasCommError =
        (rootCause instanceof ConnectException ||
          rootCause instanceof ConnectTimeoutException ||
          rootCause instanceof NoHttpResponseException ||
          rootCause instanceof SocketException);

      if (wasCommError) {
        if (--maxRetries > 0) {
          log.warn("ERROR: " + rootCause + " ... Sleeping for " + waitBeforeRetry + " seconds before re-try ...");
          Thread.sleep(waitBeforeRetry * 1000L);
          sent = sendBatch(cloudSolrServer, batch, waitBeforeRetry, maxRetries);
        } else {
          log.error("No more retries available! Add batch failed due to: " + rootCause);
          throw exc;
        }
      }
    }

    batch.clear();

    return sent;
  }

  static void displayOptions(PrintStream out) throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GCLogAnalyzer.class.getName(), getOptions());
  }

  static Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] toolOpts = options();
    for (int i = 0; i < toolOpts.length; i++)
      options.addOption(toolOpts[i]);
    return options;
  }

  public static CommandLine processCommandLineArgs(Options options, String[] args) {
    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(GCLogAnalyzer.class.getName(), options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(GCLogAnalyzer.class.getName(), options);
      System.exit(0);
    }

    return cli;
  }
}
