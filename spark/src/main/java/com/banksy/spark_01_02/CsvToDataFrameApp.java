package com.banksy.spark_01_02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

/**
 * 提取CSV文件
 * @Author banksy
 * @Data 2022/9/11 10:51 PM
 * @Version 1.0
 **/
public class CsvToDataFrameApp {
    @Test
    public static void main(String[] args) {
        CsvToDataFrameApp app = new CsvToDataFrameApp();
        app.start();
    }
    private void start(){
        //在本地主服务器创建会话
        SparkSession spark = SparkSession.builder().appName("CSV to Dataset").master("local").getOrCreate();
        //使用表头（header）读取CSV文件，调用books.csv，将其存储到数据帧中
        Dataset<Row> df = spark.read().format("csv").option("haeder", "true").load("spark/data/books.csv");
        //数据帧最多显示5行
        df.show(5);
    }
}