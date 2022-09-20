package com.banksy.spark_03;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * 数据帧中的 json 提取和操作
 * @Author banksy
 * @Data 2022/9/13 8:45 PM
 * @Version 1.0
 **/
public class JsonIngestionSchemaManipulationApp {
    @Test
    public static void main(String[] args) {
        JsonIngestionSchemaManipulationApp app = new JsonIngestionSchemaManipulationApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Durham County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("json")
                .load("spark/data/Restaurants_in_Durham_County_NC.json");
        System.out.println("*** 提取前：");
        df.show(5);
        df.printSchema();
        System.out.println("共有 " + df.count() + " 行数据.");

        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type", split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));
        df = df.withColumn("id",
                concat(df.col("state"), lit("_"),
                        df.col("county"), lit("_"),
                        df.col("datasetId")));

        System.out.println("*** 转换后：");
        df.show(5);
        df.printSchema();

        System.out.println("*** 查看分区：");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);

        df = df.repartition(4);
        System.out.println("Partition count after repartition: " +
                df.rdd().partitions().length);
    }
}