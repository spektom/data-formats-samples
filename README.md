data-formats-samples
====================

Apache Spark-based generator for data samples in different formats with different compressions.
This work was derived from Apache Spark's unit test suite.

Supported formats:

 - orc
 - parquet
 - avro
 - json
 - csv
 - tsv
 - psv
 
Supported compressions are all that are supported by Spark and current format.
 
## Building

    mvn package
    
## Running

    spark-submit target/data-foramts-samples_2.11-0.1.0-uberjar.jar
    
After running, a folder named `output` will contain the data files.

