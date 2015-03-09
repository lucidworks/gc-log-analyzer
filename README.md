Java GC Log Analyzer
========

Java GC log parser / analyzer for Java 7 and 8


Getting Started
========

1) Build the project using Maven:

```
mvn clean package
```

2) Enable the following flags on the Java process you want to analyze logs for:

```
-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCCause \
-XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution \
-XX:+PrintGCApplicationStoppedTime \
-XX:+UseConcMarkSweepGC \
-XX:+UseParNewGC \
-Xloggc:/var/solr/logs/solr_gc.log
```

The `-Xloggc` setting sets the location where Java will write the GC log, please update that parameter for your system;
you'll need to pass the path to this file when invoking the analyzer in step 4 below.

Currently, the parser only works with the CMS and ParNew collectors on Java 7 or 8, consequently, you can only
analyze logs for JVMs run with these flags set: `-XX:+UseConcMarkSweepGC -XX:+UseParNewGC`

NOTE: Support for G1 is under-construction and will be added in the future.

FWIW, here are the full GC tuning settings I'm using successfully to support complex faceting / sorting queries in Solr:

```
-Djava.net.preferIPv4Stack=true \
-Duser.timezone=UTC \
-Xloggc:/var/solr/logs/solr_gc.log \
-XX:+PrintGCApplicationStoppedTime \
-XX:+PrintTenuringDistribution \
-XX:+PrintGCDateStamps \
-XX:+PrintGCCause \
-XX:+PrintGCDetails \
-XX:+PrintHeapAtGC \
-verbose:gc \
-XX:CMSTriggerPermRatio=80 \
-XX:CMSFullGCsBeforeCompaction=1 \
-XX:+ParallelRefProcEnabled \
-XX:+CMSParallelRemarkEnabled \
-XX:CMSMaxAbortablePrecleanTime=6000 \
-XX:CMSInitiatingOccupancyFraction=50 \
-XX:+UseCMSInitiatingOccupancyOnly \
-XX:PretenureSizeThreshold=64m \
-XX:+CMSScavengeBeforeRemark \
-XX:ParallelGCThreads=6 \
-XX:ConcGCThreads=6 \
-XX:+UseParNewGC \
-XX:+UseConcMarkSweepGC \
-XX:MaxTenuringThreshold=8 \
-XX:TargetSurvivorRatio=90 \
-XX:SurvivorRatio=4 \
-XX:NewRatio=2 \
-Xmx12g \
-Xms12g \
-Xss256k
```

3) Optionally, setup a SolrCloud collection to index GC event data for deeper analysis with Fusion/Banana.

4) Run the GC log analyzer command-line application in tail mode and send events into Solr:

```
java -jar target/gc-log-analyzer-0.1-exe.jar \
  -tail \
  -log /var/solr/logs/solr_gc.log \
  -javaHostAndPort localhost:8983 \
  -javaPid 1234 \
  -javaVers 1.7 \
  -zkHost localhost:2181 \
  -collection my_gc_logs
```

Notice that you need to pass some basic information about the JVM process that created the log. This information is
simply passed through in the docs sent to Solr so you can keep track of your GC logs on multiple machines in a cluster.

For more information about all the command-line options, simply do:

```
java -jar target/gc-log-analyzer-0.1-exe.jar -help
```

Please send any questions you have about this application to tim.potter@lucidworks.com

Happy GC tuning!
