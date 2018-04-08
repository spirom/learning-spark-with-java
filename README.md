# Learning Spark with Java

This project contains snippets of Java code for illustrating various
Apache Spark concepts. It is
intended to help you _get started_ with learning Apache Spark (as a _Java_ programmer)
by providing a super easy on-ramp that _doesn't_ involve cluster configuration,
building from sources or installing Spark or Hadoop. Many of these activities will be
necessary later in your learning experience,
after you've used these examples to achieve basic familiarity.

The project is intended to accompany a number of posts on the blog
[A River of Bytes](http://www.river-of-bytes.com).

The basic approach used in this project is to create multiple small, free-standing example
programs that each illustrate an aspect fo Spark usage, and to use code comments to explain as
many details as seems useful to beginning Spark programmers.

## Dependencies

The project is based on Apache Spark 2.2.0 and Java 8.

*Warning: In Spark 2.2, support for Java 7 is finally gone.
This is documented in the [Spark 2.2.0 release notes](http://spark.apache.org/releases/spark-release-2-2-0.html),
but alas not in the corresponding
[JIRA ticket -- Spark 19493](https://issues.apache.org/jira/browse/SPARK-19493).*

## Related projects

This project is derived from the
[LearningSpark project](https://github.com/spirom/LearningSpark) which had the same goals but for
Scala programmers. In that project you can also find the early Java 7 examples that gave
rise to this project: A lot of Spark programming is a lot less painful in Java 8 than in Java 7.

The [spark-streaming-with-kafka](https://github.com/spirom/spark-streaming-with-kafka) project is
based on Spark's Scala APIs and illustrates the use of Spark with Apache Kafka, using a similar
approach: small free-standing example programs.

The [spark-data-sources](https://github.com/spirom/spark-data-sources) project is focused on
the new experimental APIs introduced in Spark 2.3.0 for developing adapters for
external data sources of
various kinds. This API is essentially a Java API (developed in Java) to avoid forcing
developers to adopt Scala for their data source adapters. Consequently, the example data sources
in this project are written in Java, but both Java and Scala usage examples are provided.

## Contents

| Package | What's Illustrated    |
|---------|-----------------------|
| [rdd](src/main/java/rdd) | The JavaRDD: core Spark data structure -- see the local README.md in that directory for details. |
| [pairs](src/main/java/pairs) | A special RDD for the common case of pairs of values -- see the local README.md in that directory for details. |
| [dataset](src/main/java/dataset) | A range of Dataset examples (queryable collection that is statically typed) -- see the local README.md in that directory for details. |
| [dataframe](src/main/java/dataframe) | A range of DataFrame/Dataset<Row> examples (queryable collection that is dynamically typed) -- see the local README.md in that directory for details. |
| [streaming](src/main/java/streaming) | A range of streaming examples -- see the local README.md in that directory for details. |
