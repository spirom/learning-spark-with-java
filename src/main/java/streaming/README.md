# Streaming

** Utilities

<table>
<tr>
<th>File</th>
<th>Purpose</th>
</tr>
<tr>
<td>CVSFileStreamGenerator.java</td>
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
</table>

## Getting started

<table>
<tr>
<th>File</th>
<th>What's Illustrated</th>
</tr>
<tr>
<td>FileBased.java</td>
<td>How to create a stream of data from files appearing in a directory. <b>Start here.</b>

## Processing the Data

<table>
<tr>
<th>File</th>
<th>What's Illustrated</th>
</tr>
<tr>
<td>MultipleTransformations.java<td>
<td><p>How to establish multiple streams on the same source of data and register multiple processing
functions on a single stream.</p></td>
</tr>
</table>
