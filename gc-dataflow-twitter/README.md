# Dataflow Twitter

Dataflow (Apache Beam) pipeline to:
    - read tweets from Kafka (1 per line, json)
    - extract and count entities (sliding windows)
    - output in Kafka (csv: timestamp, entity, count)

Run on Flink and Google Cloud, with minor changes due to current limitations. Limited on Spark. Include tests.


## Apache Flink

Support for Dataflow 1.0.0 (current 1.4.0).

No support for writing in db.

TODO:
    - cleanup code and comments
    - make Flink-specific code more modular
