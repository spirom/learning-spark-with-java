package streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import streaming.util.CSVFileStreamGenerator;

import java.io.File;
import java.io.IOException;

/**
 * This example uses an accumulator to keep a running total of the number of records processed. Every batch
 * that is processed is added to it, and the running total is printed.
 */
public class StateAccumulation {

  static class RecordCounter {

    private static volatile LongAccumulator instance = null;

    public static void clobber() {
      instance = null;
    }

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
      if (instance == null) {
        synchronized (RecordCounter.class) {
          if (instance == null) {
            System.out.println("*** Initializing RecordCounter");
            instance = jsc.sc().longAccumulator("RecordCounter");
          }
        }
      }
      return instance;
    }
  }

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

    // use the utility class to produce a sequence of 10 files, each containing 100 records
    CSVFileStreamGenerator fm = new CSVFileStreamGenerator(10, 100, 500);

    JavaDStream<String> streamOfRecords = ssc.textFileStream(fm.getDestination().getAbsolutePath());

    streamOfRecords.foreachRDD(rdd -> {
      // The getInstance() pattern ensures that it is only initialized for the first batch. It will also
      // be initialized on recovery if you use checkpointing, but it's state won't be recovered for you.
      final LongAccumulator recordCounter = RecordCounter.getInstance(new JavaSparkContext(rdd.context()));
      long records = rdd.count();
      recordCounter.add(records);
      System.out.println("This RDD: " + records + " running total: " + recordCounter.value());
    });

    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();


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
    System.out.println("*** Streaming context terminated");

  }



}
