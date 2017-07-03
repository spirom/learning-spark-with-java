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
 * This example adds to the simple example in FileBased.java by introducing stream transformations
 * and by registering multiple batch processing functions on the various streams.
 * You can think of the relationships between them all as follows:
 *    -- root stream of records from the text files
 *      -- registered function [1]: print the count of records in each batch RDD
 *      -- derived stream by calling count(): each RDD contains the count of elements in the batch
 *         -- registered function [2]: print the count (should give same results as [1])
 *      -- derived stream by calling map() to parse the records int he CSV files
 *         -- registered function [3]: print the value of any object with key "Key_40"
 *         -- registered function [4]: print the fraction of objects whose value is negative
 *  Because the various outputs tend to get tangled up, the output of each registered function identifies itself by a
 *  bracketed number: [1], [2], [3] or [4].
 */

public class MulitpleTransformations {
  public static void main(String[] args) {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-MultipleTransformations")
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

    // Register on function to be applied to each RDD -- counting the elements and printing the count;
    // in this case we'll also print the timestamp every time an RDD is received -- notice we provide a function
    // that takes two parameters this time: the RDD and the time stamp
    streamOfRecords.foreachRDD((rdd, timeStamp) -> {
        // NOTE: the [1] below will identify every line printed by this function
        System.out.println("[1] Timestamp: " + timeStamp + " Count: " + rdd.count());
    });

    // By calling count() on the stream we transform it another stream, this time a JavaDStream<Long>, that
    // where each RDD contains a single element whose value is the count of elements in each RDD produced by
    // the original stream -- so if we register a function to print those values, it will produce the same data
    // as the function above that counts elements of RDDs int he original stream
    streamOfRecords.count().foreachRDD((rdd, timeStamp) ->
      rdd.foreach(countValue ->
          // NOTE: the [1] below will identify every line printed by this function
          System.out.println("[2] Timestamp: " + timeStamp + " Count: " + countValue)
      )
    );

    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of StreamingItem objects
    JavaDStream<StreamingItem> streamOfItems = streamOfRecords.map(s -> new StreamingItem(s));

    // use the stream objects to print the values whose key is Key_40
    streamOfItems.foreachRDD(rdd -> {
      // NOTE: the [3] below will identify every line printed by this function
      rdd.foreach(item -> {
        // NOTE: since a batch may contain more than one file,
        // and each file will contain a Key_50, this may print more than once per batch
        if (item.getKey().equals("Key_40")) System.out.println("[3] Key_40 = " + item.getValue());
      });
    });

    // Also use the stream of StreamignItem objects to calculate the fraction of negative values in each batch
    // (since the values were pseudo-random, it will often be around 0.5 or so)
    streamOfItems.foreachRDD(rdd -> {
      // NOTE: the [4] below will identify every line printed by this function

      if (rdd.count() > 0) {
        double negativeCount = rdd.filter(item -> item.getValue() < 0).count();
        double fraction = negativeCount / rdd.count();
        System.out.println("[4] negative fraction = " + fraction);
      }
    });

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
