Spark Example Run

/mnt/softwares/spark-1.1.1-bin-hadoop1/bin/spark-submit --class org.apache.spark.examples.SparkPi --master local[4] --executor-memory 2G --executor-cores 4 /mnt/softwares/spark-1.1.1-bin-hadoop1/lib/spark-examples-1.1.1-hadoop1.0.4.jar 10

Spark Cassandra Run:

/mnt/softwares/spark-1.3.0-bin-hadoop1/bin/spark-submit --class com.sparkscala.connections.SprakCassandra --master spark://sany:7077 --executor-memory 4G --executor-cores 2  --jars "/mnt/softwares/spark-1.3.0-bin-hadoop1/spark-cassandra-connector_2.10-1.2.0-rc1.jar","/mnt/softwares/spark-1.3.0-bin-hadoop1/cassandra-driver-core-2.1.3.jar","/mnt/softwares/spark-1.3.0-bin-hadoop1/cassandra-thrift-2.1.3.jar","/mnt/softwares/spark-1.3.0-bin-hadoop1/guava-17.0.jar","/mnt/softwares/spark-1.3.0-bin-hadoop1/joda-time-2.6.jar" /home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar


Spark Hbase Run:
/mnt/softwares/spark-1.3.0-bin-hadoop1/bin/spark-submit --class com.sparkscala.connections.SparkHbase --master spark://sany:7077 --executor-memory 4G --executor-cores 2  /home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar

Spark Hive Run:
/mnt/softwares/spark-1.3.0-bin-hadoop1/bin/spark-submit --class com.sparkscala.connections.SparkHive --master spark://sany:7077 --executor-memory 4G --executor-cores 2  /home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar

SparkQL Run:
/mnt/softwares/spark-1.3.0-bin-hadoop1/bin/spark-submit --class com.sparkscala.connections.SparkQLL --master spark://sany:7077 --executor-memory 4G --executor-cores 2  /home/sany/Documents/Operations/SparkScala/target/SparkScala-0.0.1-SNAPSHOT.jar