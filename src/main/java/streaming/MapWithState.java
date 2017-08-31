package streaming;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import streaming.util.CSVFileStreamGenerator;
import streaming.util.StreamingItem;

import java.io.File;
import java.io.IOException;


public class MapWithState {
  public static void main(String[] args) {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Filtering")
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
    // create the stream, which will contain the rows of the individual files as strings
    // -- notice we can create the stream even though this directory won't have any data until we call
    // fm.makeFiles() below
    JavaDStream<String> streamOfRecords = ssc.textFileStream(fm.getDestination().getAbsolutePath());

    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of KeyAndValue objects
    JavaDStream<StreamingItem> streamOfItems = streamOfRecords.map(s -> new StreamingItem(s));

    JavaPairDStream<StreamingItem.Category, StreamingItem> streamOfPairs =
        streamOfItems.mapToPair(si ->
            new Tuple2<>(si.getCategory(), si));

    Function3<StreamingItem.Category, Optional<StreamingItem>, State<Integer>, Tuple2<StreamingItem.Category, Integer>>
      mappingFunction = (category, item, state) -> {
        int count = 1 + (state.exists() ? state.get() : 0);
        Tuple2<StreamingItem.Category, Integer> thisOne = new Tuple2<>(category, count);
        state.update(count);
        return thisOne;
    };

    JavaMapWithStateDStream<StreamingItem.Category, StreamingItem, Integer, Tuple2<StreamingItem.Category, Integer>> streamOfCategoryCounts =
        streamOfPairs.mapWithState(StateSpec.function(mappingFunction));

    streamOfCategoryCounts.foreachRDD(rdd -> {
      System.out.println("Batch size: " + rdd.count());
      rdd.foreach(e -> System.out.println(e));
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
