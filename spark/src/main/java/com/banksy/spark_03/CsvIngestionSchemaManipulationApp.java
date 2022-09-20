package com.banksy.spark_03;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * 数据帧中的 CSV 提取和操作
 * @Author banksy
 * @Data 2022/9/13 8:06 PM
 * @Version 1.0
 **/
public class CsvIngestionSchemaManipulationApp {
    @Test
    public static void main(String[] args) {
        CsvIngestionSchemaManipulationApp app = new CsvIngestionSchemaManipulationApp();
        app.start();
    }
    private void start(){
        SparkSession spark = SparkSession.builder().appName("Restaurants in Wake County, NC").master("local").getOrCreate();
        Dataset<Row> df = spark.read().format("csv").option("header", "true").load("spark/data/Restaurants_in_Wake_County_NC.csv");
        System.out.println("*** 前5行数据：");
        df.show(5);
        System.out.println("*** 数据帧模式：");
        df.printSchema();
        System.out.println("有" + df.count() + " 行数据！");

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        df = df.withColumn("id", concat(
                                          df.col("state"),lit("_"),
                                          df.col("county"), lit("_"),
                                          df.col("datasetId")));
        System.out.println("*** 数据格式修改后：");
        df.show(5);

        Dataset<Row> dfUsedForBook = df.drop("address2")
                .drop("zip")
                .drop("tel")
                .drop("dateStart")
                .drop("geoX")
                .drop("geoY")
                .drop("address1")
                .drop("datasetId");
        System.out.println("删除一些列后：");
        dfUsedForBook.show(5, 15);
        df.printSchema();


        System.out.println("*** 查看分区个数");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("重新分区前的分区计数: " + partitionCount);
        df = df.repartition(4);
        System.out.println("重新分区后的分区计数: " + df.rdd().partitions().length);
    }
}