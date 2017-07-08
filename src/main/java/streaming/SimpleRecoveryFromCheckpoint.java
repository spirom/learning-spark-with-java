package streaming;


import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import streaming.util.CSVFileStreamGenerator;

import java.io.File;
import java.io.IOException;

/**
 * This example demonstrates how to persist configured JavaDStreams across a failure and restart. It simulates
 * failure by destroying the first streaming context (for which a checkpoint directory is configured) and
 * creating a second one, not from scratch, but by reading the checkpoint directory. The
 * JavaStreamingContext.getOrCreate() method is used somewhat artificially here, since the second argument would
 * normally be a function that creates the streaming context, so that the same initialization call could be used when
 * checkpoint data doesn't exist and when it does. That design pattern is adequately demonstrated in the
 * documentation: the goal here is more to explain how it works, so the initialization function provided always throws
 * exception to demonstrate that it isn't called a checkpoint is always available.
 *
 * Other than initializing the streaming context from the checkpoint file, the other key point to notice in this code
 * is that after recovery, the new streaming context already has the previous context's stream configured: there's no
 * need to call ssc2.textFileStream() to configure one, or even to call forEachRDD() again on anything.
 */
public class SimpleRecoveryFromCheckpoint {

  public static void main(String[] args) {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-FileBased")
        .master("local[4]")
        .getOrCreate();

    //
    // Operating on a raw RDD actually requires access to the more low
    // level SparkContext -- get the special Java version for convenience
    //
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
    // but it isn't -- it comes from org.apache.spark.streaming)
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));

    String checkpointPath = File.separator + "tmp" + File.separator + "LSWJ" + File.separator + "checkpoints";
    File checkpointDir = new File(checkpointPath);
    checkpointDir.mkdir();
    checkpointDir.deleteOnExit();
    ssc.checkpoint(checkpointPath);

    // use the utility class to produce a sequence of 10 files, each containing 100 records
    CSVFileStreamGenerator fm = new CSVFileStreamGenerator(10, 100, 500);

    // normally we would use a call to JavaStreamingContext.getOrCreate() to initialize a streaming context
    // where want to sue recovery, but here we are simulating recovery, so let's just initialize it from scratch
    JavaDStream<String> streamOfRecords = ssc.textFileStream(fm.getDestination().getAbsolutePath());

    streamOfRecords.foreachRDD(rdd -> {
      long records = rdd.count();
      System.out.println("[1] Records in this RDD: " + records);
    });

    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();

    // send some data through
    Thread t = new Thread() {
      public void run() {
        try {
          Thread.sleep(2000);

          System.out.println("*** producing data");
          // start producing files
          fm.makeFiles();

          // give it time to get processed
          Thread.sleep(10000);
        } catch (InterruptedException ie) {
        } catch (IOException ioe) {
          throw new RuntimeException("problem in background thread", ioe);
        }

        ssc.stop(false, true);
        System.out.println("*** stopping streaming");
      }
    };
    t.start();

    try {
      ssc.awaitTermination();
    } catch (InterruptedException ie) {

    }
    System.out.println("*** First streaming context terminated");

    // simulate failure and recovery within a signel process execution by using a completely
    // new streaming context below

    // this will recover the stream we configuyred above -- no need to configure it again
    JavaStreamingContext ssc2 = JavaStreamingContext.getOrCreate(checkpointPath, () -> {
      // this would normally contain code to initialize the streaming from scratch because no checkpoint
      // was found, but we know that one WILL be found, and so we prove it by making this always throw!
      System.out.println("*** shouldn't be getting here: trying to re-create streaming context");
      throw new IllegalStateException("");
    });

    // start streaming
    System.out.println("*** about to start streaming again");
    ssc2.start();

    // send some more data through the recovered stream
    Thread t2 = new Thread() {
      public void run() {
        try {
          Thread.sleep(2000);

          System.out.println("*** producing data");
          // start producing files
          fm.makeFiles();

          // give it time to get processed
          Thread.sleep(10000);
        } catch (InterruptedException ie) {
        } catch (IOException ioe) {
          throw new RuntimeException("problem in background thread", ioe);
        }
        ssc2.stop(false, true);
        System.out.println("*** stopping streaming again");
      }
    };
    t2.start();

    try {
      ssc2.awaitTermination();
    } catch (InterruptedException ie) {

    }
    System.out.println("*** Second streaming context terminated");


  }


}


