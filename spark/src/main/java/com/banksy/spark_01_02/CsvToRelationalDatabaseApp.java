package com.banksy.spark_01_02;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Properties;

/**
 * 提取CSV文件，转换数据并将数据保存在数据库中
 * @Author banksy
 * @Data 2022/9/12 9:52 AM
 * @Version 1.0
 **/
public class CsvToRelationalDatabaseApp {
    @Test
    public static void main(String[] args) {
        CsvToRelationalDatabaseApp spark = new CsvToRelationalDatabaseApp();
        spark.start();
    }
    private void start(){
        //在本地主服务器创建会话
        SparkSession spark = SparkSession.builder().appName("CSV to DB").master("local").getOrCreate();
        //使用表头（header）读取CSV文件，调用authors.csv，将其存储到数据帧中
        Dataset<Row> df = spark.read().format("csv").option("header", "true").load("spark/data/authors.csv");
        //创建名为"name"的新列，将lname列包含，的虚拟列，和fname列串联起来
        df = df.withColumn("name",concat(df.col("lname"), lit(", "), df.col("fname")));
        //连接数据库
        String dbConnectionUrl = "jdbc:mysql://172.16.123.11:3306/spark?serverTimezone=UTC&useSSL=false&useUnicode=true&characterEncoding=utf8";
        Properties prop = new Properties();
        prop.setProperty("driver","com.mysql.cj.jdbc.Driver");
        prop.setProperty("user","root");
        prop.setProperty("password","123456");
        //重写名为authors的表
        df.write().mode(SaveMode.Overwrite).jdbc(dbConnectionUrl, "authors", prop);
        System.out.println("Process complete!");
    }
}