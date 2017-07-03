package streaming;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import streaming.util.CSVFileStreamGenerator;


/**
 * File based streaming requires files to be atomically created in
 * the source directory -- in practice this entails creating them somewhere
 * else and renaming them in place.
 *
 * The streaming context is set up to process a batch of new data once per second. Each batch is a single RDD
 * containing one entry for each text line of the newly discovered files in the specified directory since the
 * last batch was processed. Since it's possible for more than one file to appear in the directory in a batch
 * interval, one such RDD may contain the data from more than one file.
 *
 */

public class FileBased {
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
    // create the stream, which will contain the rows of the individual files as strings
    // -- notice we can create the stream even though this directory won't have any data until we call
    // fm.makeFiles() below
    JavaDStream<String> streamOfRecords = ssc.textFileStream(fm.getDestination().getAbsolutePath());

    // register a function to process data from the stream -- in this case it's a very simple function
    // counts the number of elements ine ach RDD and prints it
    streamOfRecords.foreachRDD(r -> {
      System.out.println(r.count());
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
