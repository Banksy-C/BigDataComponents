package com.banksy.spark_04;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.apache.spark.sql.functions.expr;

/**
 * @Author banksy
 * @Data 2022/9/19 9:23 PM
 * @Version 1.0
 **/
public class TransformationExplainApp {
    @Test
    public static void main(String[] args) {
        TransformationExplainApp app = new TransformationExplainApp();
        app.start();
    }

    private void start(){
        // Step 1 - Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Showing execution plan")
                .master("local")
                .getOrCreate();

        // Step 2 - Reads a CSV file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load(
                        "spark/data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
        Dataset<Row> df0 = df;

        // Step 3 - Build a bigger dataset
        df = df.union(df0);

        // Step 4 - Cleanup. preparation
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        // Step 5 - Transformation
        df = df
                .withColumn("avg", expr("(lcl+ucl)/2"))
                .withColumn("lcl2", df.col("lcl"))
                .withColumn("ucl2", df.col("ucl"));

        // Step 6 - explain - Spark v2
        df.explain();

        // Step 6 - explain - Spark v3

        // simple
        // df.explain("simple");

        // extended
        // df.explain("extended");

        // codegen
        // df.explain("codegen");

        // cost
        // df.explain("cost");

        // formatted
        //df.explain("formatted");
    }
}