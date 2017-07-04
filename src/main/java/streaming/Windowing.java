package streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import streaming.util.CSVFileStreamGenerator;
import streaming.util.StreamingItem;

import java.io.IOException;

/**
 * The windowing methods on a stream allow you to create a derived stream whose batches contain data from some
 * number fo the most recent batches in the parent stream, and producing a batch per some number of parent
 * stream batches (default is one.)
 *
 * Both the sliding window size and the batch frequency are specified as durations, which must be integer
 * multiples of the parent stream's batch duration. Of course, the parent stream could itself have been
 * derived from another stream, so its batch duration will not necessarily be the duration specified for the
 * JavaStreamingContext.
 *
 * This example creates two derived streams with different window and slide durations. All three streams print
 * their batch size every time they produce a batch, so you can compare the number of records across streams
 * and batches.
 */

public class Windowing {
  public static void main(String[] args) {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Windowing")
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
    // create the stream, which will contain the rows of the individual files as strings
    // -- notice we can create the stream even though this directory won't have any data until we call
    // fm.makeFiles() below
    JavaDStream<String> streamOfRecords = ssc.textFileStream(fm.getDestination().getAbsolutePath());

    // Create a derived stream that will produce a batch every second (just like its parent) but the
    // batch will contain data from the parent stream's most recent two batches
    JavaDStream<String> threeSecondsEverySecond = streamOfRecords.window(new Duration(3000));

    // Another derived stream, this time creating batch every two seconds, but containing the data from
    // the parent stream's most recent five batches
    JavaDStream<String> fiveSecondsEveryTwoSeconds =
        streamOfRecords.window(new Duration(5000), new Duration(2000));

    //
    // Register functions to print the batch sizes in all three of the streams. Notice that:
    // 1) Each stream identifies itself as either [original], [window 3s] or [window 3s slide 2s]
    // 2) The "window duration" is how far back int he parent's stream is included in very batch
    // 3) The "slide duration" is how often a "windowed" batch is produced byt he derived stream (default is the
    //    same as the parent's "batch duration")
    // 4) You will see output from the [window 3s] stream every second, but only every two seconds from
    //    [window 3s slide 2s] -- and you can check the item counts against the right number of most recent item
    //    counts from the [original] stream.
    // 5) Time stamps are included to help you keep track of the batches
    //

    streamOfRecords.foreachRDD((rdd, timeStamp) ->
        System.out.println("[original] TS: " + timeStamp + " Item count = " + rdd.count()));

    threeSecondsEverySecond.foreachRDD((rdd, timeStamp) ->
        System.out.println("[window 3s] TS: " + timeStamp + " Item count = " + rdd.count()));

    fiveSecondsEveryTwoSeconds.foreachRDD((rdd, timeStamp) ->
        System.out.println("[window 5s slide 2s] TS: " + timeStamp + " Item count = " + rdd.count()));

    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();


    Thread t = new Thread() {
      public void run() {
        try {
          // A curious fact about files based streaming is that any files written
          // before the first RDD is produced are ignored. So wait longer than
          // that before producing files.
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
        ssc.stop();
        System.out.println("*** stopping streaming");
      }
    };
    t.start();

    try {
      ssc.awaitTermination();
    } catch (InterruptedException ie) {

    }
    System.out.println("*** Streaming terminated");
  }

}
