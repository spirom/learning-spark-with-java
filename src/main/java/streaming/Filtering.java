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
 * Much of the processing we require on streams is agnostic about batch boundaries. It's convenient to have
 * methods on JavaDStream that allow us to transform the streamed data item by item (using map()), or filter it
 * item by item (using filter()) without being concerned about batch boundaries as embodied by individual RDDs.
 * This example again uses map() to parse the records int he ext files and then filter() to filter out individual
 * entries, so that by the time we receive batch RDDs only the desired items remain.
 *
 * Of course, similar filtering and transformation methods are available on the JavaRDD class, and its better to
 * use those in the case where your algorithm NEEDS to be aware of batch boundaries. The methods on JavaDStream
 * illustrated here are useful exactly because they abstract batch boundaries away, and also because they
 * create a stream that can be used for additional processing.
 *
 * On the other hand, if you want to transform data in a way that is aware of batch boundaries but still creates a
 * stream, you can uses transform() and similar methods on JavaDStream that are illustrated elsewhere.
 */

public class Filtering {
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

    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of KeyAndValue objects
    JavaDStream<StreamingItem> streamOfItems = streamOfRecords.map(s -> new StreamingItem(s));

    // create a derived stream that only contains StreamingItem objects whose category value is MEDIUM
    JavaDStream<StreamingItem> streamOfMediumEntries =
        streamOfItems.filter(item -> item.getCategory() == StreamingItem.Category.MEDIUM);

    // now register a function to print the size of each batch -- notice there are fewer items in each one
    // as only the MEDIUM entries have been retained.
    streamOfMediumEntries.foreachRDD(rdd -> System.out.println("Item count = " + rdd.count()));

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
