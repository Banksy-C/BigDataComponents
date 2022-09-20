package com.banksy.spark_03;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * java数组中创建String数据集-->再从数据集转换为数据帧
 * @Author banksy
 * @Data 2022/9/14 8:56 PM
 * @Version 1.0
 **/
public class ArrayToDatasetToDataframeApp {
    @Test
    public static void main(String[] args) {
        ArrayToDatasetToDataframeApp app = new ArrayToDatasetToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Array to Dataset<String> to dataframe")
                .master("local")
                .getOrCreate();

        //字符串数组
        String[] stringList = new String[] { "Jean", "Liz", "Pierre", "Lauric" };
        List<String> data = Arrays.asList(stringList);
        //数组转换为数据集
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
        ds.show();
        ds.printSchema();

        //数据集转换为数据帧
        Dataset<Row> df = ds.toDF();
        df.show();
        df.printSchema();
    }
}