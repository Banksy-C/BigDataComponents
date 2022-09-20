package com.banksy.spark_03;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

/**
 * StructType的printTreeString()方法，模式自我检查
 * @Author banksy
 * @Data 2022/9/13 8:37 PM
 * @Version 1.0
 **/
public class SchemaIntrospectionApp {
    @Test
    public static void main(String[] args) {
        SchemaIntrospectionApp app = new SchemaIntrospectionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Schema introspection for restaurants in Wake County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("spark/data/Restaurants_in_Wake_County_NC.csv");

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
                .withColumnRenamed("Y", "geoY");
        df = df.withColumn("id", concat(
                df.col("state"),
                lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        // NEW
        ////////////////////////////////////////////////////////////////////

        StructType schema = df.schema();

        System.out.println("*** Schema as a tree:");
        schema.printTreeString();
        String schemaAsString = schema.mkString();
        System.out.println("*** Schema as string【显示为字符串结构】: " + schemaAsString);
        String schemaAsJson = schema.prettyJson();
        System.out.println("*** Schema as JSON【显示为json结构】: " + schemaAsJson);
    }
}