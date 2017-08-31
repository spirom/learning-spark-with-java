# Streaming

## Some philosophy

<p>Spark streaming techniques fall into two broad areas that don't have much to do with each other
until you get to the advanced topics:</p>
<ul>
<li>How to get the data streamed into Spark from some external system like a database or a messaging system --
and sometimes how to get it streamed back out again.</li>
<li>How to transform the data within Spark while making full use of the streaming features.</li>
</ul>

<p>After the single "getting started" example below, we'll look at these two areas separately. Eventually there may
need to ba a section of "advanced" examples that tie them together again. </p>

<p>Of course, to transform streaming data we need to set up a streaming data source. Many of the sources you'll encounter
in practice take considerable set up, so I've chosen to use Spark's file streaming mechanism and provide a utility
class for generating a stream of files containing random data. My hope is that for most users of these examples it
will need no setup at all, and it has the useful side effect of bringing streaming "down to earth" by using
such a "low tech" mechanism.  </p>

## Utilities

<table>
<tr>
<th>File</th>
<th>Purpose</th>
</tr>
<tr>
<td valign="top">CVSFileStreamGenerator.java</td>
<td>
<p>A utility for creating a sequence of files of integers in the file system
so that Spark can treat them like a stream. This follows a standard pattern
to ensure correctness: each file is first created in another folder and then
atomically renamed into the destination folder so that the file's point of
creation is unambiguous, and is correctly recognized by the streaming
mechanism.</p>

<p>Each generated file has the same number of key/value pairs, where the
keys have the same names from file to file, and the values are random
numbers, and thus vary from file to file.</p>

<p>This class is used by several of the streaming examples.</p>
</td>
</tr>
<tr>
<td valign="top">StreamingItem.java</td>
<td><p>An item of data to be streamed. This is used to generate the records int he CSV files and
also to parse them. Several of the example stream processing pipelines will parse the text data into these
objects for further processing. </p></td>
</tr>
</table>

## Getting started

<table>
<tr>
<th>File</th>
<th>What's Illustrated</th>
</tr>
<tr>
<td valign="top">FileBased.java</td>
<td>How to create a stream of data from files appearing in a directory. <b>Start here.</b></td>
</tr>
</table>

## Processing the Data

<table>
<tr>
<th>File</th>
<th>What's Illustrated</th>
</tr>
<tr>
<td valign="top">MultipleTransformations.java</td>
<td><p>How to establish multiple streams on the same source of data and register multiple processing
functions on a single stream.</p></td>
</tr>
<tr>
<td valign="top">Filtering.java</td>
<td><p>Much of the processing we require on streams is agnostic about batch boundaries. It's convenient to have
methods on JavaDStream that allow us to transform the streamed data item by item (using map()), or filter it
item by item (using filter()) without being concerned about batch boundaries as embodied by individual RDDs.
This example again uses map() to parse the records int he ext files and then filter() to filter out individual
entries, so that by the time we receive batch RDDs only the desired items remain.</p></td>
</tr>
<tr>
<td valign="top">Windowing.java</td>
<td><p>This example creates two derived streams with different window and slide durations.
All three streams print their batch size every time they produce a batch, so you can compare the
number of records across streams and batches.</p></td>
</tr>
<tr>
<td valign="top">StateAccumulation.java</td>
<td><p>This example uses an accumulator to keep a running total of the number of records processed. Every batch
that is processed is added to it, and the running total is printed.</p></td>
</tr>
</table>


## Streaming Sources

TBD

## Advanced Topics

<table>
<tr>
<th>File</th>
<th>What's Illustrated</th>
</tr>
<tr>
<td valign="top">SimpleRecoveryFromCheckpoint.java</td>
<td><p>This example demonstrates how to persist configured JavaDStreams across a failure and restart. It simulates
failure by destroying the first streaming context (for which a checkpoint directory is configured) and
creating a second one, not from scratch, but by reading the checkpoint directory.</p></td>
</tr>
<tr>
<td valign="top">MapWithState.java</td>
<td>(In progress)</td>
</tr>
<tr>
<td valign="top">Pairs.java</td>
<td>(In progress)</td>
</tr>
</table>
