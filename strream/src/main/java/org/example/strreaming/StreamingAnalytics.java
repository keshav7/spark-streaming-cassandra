package org.example.strreaming;


import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.sql.types.DataTypes.StringType;

/****************************************************************************
 * This example is an example for Streaming Analytics in Spark.
 * It reads a real time orders stream from kafka, performs periodic summaries
 * and writes the output the a JDBC sink.
 ****************************************************************************/

public class StreamingAnalytics implements Serializable {

    public static void main(String[] args) {

        try {
            //Create the Spark Session
            SparkSession spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.host","127.0.0.1")
                    .config("spark.sql.shuffle.partitions",2)
                    .config("spark.default.parallelism",2)
                    .appName("StreamingAnalyticsExample")
                    .getOrCreate();
//
            StructType schema = new StructType()
                                    .add("orderId", StringType)
                                    .add("source", StringType)
                                    .add("eventName", StringType);

            System.out.println("Reading from Kafka..");

            Dataset<Row> rawOrdersDf = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "streaming.alerts.input")
                    //.option("startingOffsets","earliest")
                    .load();

            //Convert JSON message in Kafka to a DataStream with columns
            Dataset<Row> ordersDf = rawOrdersDf
                    .selectExpr("CAST(value AS STRING)")
                    .select(functions.from_json(
                            functions.col("value"), schema).as("order"))
                    .select("order.orderId", "order.eventName", "order.source");

//            //Using Processing Time windows
//            //Create windows of 5 seconds and compute total Order value by Product

            Dataset<Row> summaryDF = ordersDf
                    .selectExpr("eventName",
                            "1 as value")
                    .withColumn("timestamp", functions.current_timestamp())
                    .withWatermark("timestamp","5 seconds")
                    .groupBy(functions.window(
                                functions.col("timestamp"),
                             "5 seconds"),
                            functions.col("eventName"))
                    .agg(functions.sum(functions.col("value"))) ;



            summaryDF.writeStream().foreach(new CassandraDBWriterManager()).start();

//            Keep the process running
            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }



    }
}
