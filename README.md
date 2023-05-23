# General Data Engineer Tech Test

Please submit the solution as a link to a GitHub repository.

## Take Home Test

A directory in S3 contains files with two columns

1. The files contain headers, the column names are just random strings and they are not consistent across files
2. both columns are integer values
3. Some files are CSV some are TSV - so they are mixed, and you must handle this!
4. The empty string should represent 0
5. Henceforth the first column will be referred to as the key, and the second column the value
6. For any given key across the entire dataset (so all the files), there exists exactly one value that occurs an odd number of times. . E.g. you
   will see data like this:

```scala
// value 2 occurs odd number of times
1 -> 2
1 -> 3
1 -> 3
// value 4 occurs odd number of times
2 -> 4
2 -> 4
2 -> 4
```

But never like this:
```scala
// three values occur odd number of times
1 -> 2
1 -> 3
1 -> 4
// no value for this key occurs odd number of times
2 -> 4
2 -> 4
```

Write an app in Scala that takes 3 parameters:
* An S3 path (input)
* An S3 path (output)
* An AWS ~/.aws/credentials profile to initialise creds with, or param to indicate that creds should use default provider chain. Your app
will assume these creds have full access to the S3 bucket.

Then in spark local mode the app should write file(s) to the output S3 path in TSV format with two columns such that
* The first column contains each key exactly once
* The second column contains the integer occurring an odd number of times for the key, as described by 6 above
* NOTE: If your CV doesn’t mention AWS you can assume local filesystem and not bother with the S3 stuff.

## How to run it

### Locally

```agsl
sbt assembly
cd target/scala-2.12
java -jar data-engineer-test-assembly-0.0.1.jar s3a://testdataignaciomosca/source.csv s3a://testdataignaciomoscaoutput/out default
```

### EMR

spark-submit data-engineer-test-assembly-0.0.1.jar s3a://testdataignaciomosca/source.csv s3a://testdataignaciomoscaoutput/out default



## Bonus 1

This is a bonus and entirely optional. If you have already spent a long time on the above, it’s recommended you skip this as we do not want to
consume too much time of the candidates.Provide more than one implementation of the algorithm, and as comments discuss the time & space complexity.

---

### `dataframeSolution` Analysis

#### Time Complexity:

**Reading Input Files** The time complexity of reading the input files depends on the number of files and the size of each file. Let's assume there are n input files and the average size of each file is m. In this case, the time complexity is O(n * m).

**Replacing Empty Strings** The operation to replace empty strings with 0 in the value column is performed on the DataFrame. It operates on each row individually, so the time complexity is O(N), where N is the total number of rows in the DataFrame.

**Grouping and Counting** The grouping and counting operation is performed on the DataFrame. It involves shuffling and aggregation, which has a time complexity of O(N), where N is the total number of rows in the DataFrame.

**Filtering Odd Counts** The operation to filter out values with even counts is performed on the DataFrame. It operates on each row individually, so the time complexity is O(N), where N is the number of rows in the DataFrame.

**Selecting Columns** The operation to select the key and value columns is performed on the DataFrame. It operates on each row individually, so the time complexity is O(N), where N is the number of rows in the DataFrame.

**Writing Output** The time complexity of writing the output DataFrame as TSV files depends on the number of partitions and the size of each partition. Let's assume there are p partitions and the average size of each partition is q. In this case, the time complexity is O(p * q).

Overall, the dominant time complexity is determined by the steps involving reading the input files and writing the output files. The other operations have a linear time complexity based on the number of rows in the DataFrame.

#### Space Complexity:

**Input Files** The space complexity for reading the input files depends on the total size of the input data. It requires enough memory to hold the data while processing.

**DataFrame** The space complexity for the DataFrame depends on the number of columns and the number of rows. It requires memory to store the schema, column data, and any intermediate data during processing. The space complexity is typically determined by the size of the largest partition.

**Output Files** The space complexity for writing the output files depends on the total size of the output data. It requires memory to buffer and write the data.

Overall, the space complexity is primarily determined by the size of the input and output data. The DataFrame and any intermediate data during processing also contribute to the memory usage.

<br>

---
### `sparkSQL` Analysis

**Time Complexity** The time complexity of this implementation depends on the size of the input data and the number of unique key-value pairs. The SQL query performs a group by operation, which requires shuffling and sorting the data. Thus, the time complexity can be considered O(n log n), where n is the total number of records in the input data.

**Space Complexity** The space complexity of this implementation is primarily determined by the intermediate data structures created during the SQL query execution. The space required will depend on the number of unique key-value pairs and the number of odd occurrences. Additionally, Spark SQL might use additional memory for shuffling and sorting operations. Overall, the space complexity can be considered O(k + m), where k is the number of unique key-value pairs and m is the number of odd occurrences.

---

## Bonus 2

This is a bonus and entirely optional. If you have already spent a long time on the above, it’s recommended you skip this as we do not want to
consume too much time of the candidates.
Instead of running the app in local mode, use the Java SDK for AWS "com.amazonaws" % "aws-java-sdk" % "1.11.698", to run the
app in an EMR cluster. Here the creds passed in are assumed to be admin creds for the AWS account.