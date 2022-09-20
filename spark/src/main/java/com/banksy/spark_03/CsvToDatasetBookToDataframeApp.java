package com.banksy.spark_03;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import com.banksy.spark_03.pojo.Book;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * 提取CSV文件读取到数据帧中，将数据帧转换为书籍书籍集，然后将数据集转换回数据帧
 * @Author banksy
 * @Data 2022/9/14 9:13 PM
 * @Version 1.0
 **/
public class CsvToDatasetBookToDataframeApp implements Serializable {
    private static final long serialVersionUID = -1L;

    class BookMapper implements MapFunction<Row, Book> {
        private static final long serialVersionUID = -2L;

        @Override
        public Book call(Row value) throws Exception {
            Book b = new Book();
            b.setId(value.getAs("id"));
            b.setAuthorId(value.getAs("authorId"));
            b.setLink(value.getAs("link"));
            b.setTitle(value.getAs("title"));

            // date case
            String dateAsString = value.getAs("releaseDate");
            if (dateAsString != null) {
                SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
                b.setReleaseDate(parser.parse(dateAsString));
            }
            return b;
        }
    }


    public static void main(String[] args) {
        CsvToDatasetBookToDataframeApp app = new CsvToDatasetBookToDataframeApp();
        app.start();
    }


    private void start(){
        SparkSession spark = SparkSession.builder()
                .appName("CSV to dataframe to Dataset<Book> and back")
                .master("local")
                .getOrCreate();
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");
        String filename = "spark/data/books.csv";

        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")//自动推断列类型
                .option("header", "true")//表头行
                .load(filename);
        System.out.println("*** 提取文件读取到数据帧中：");
        df.show(5);
        df.printSchema();

        Dataset<Book> bookDs = df.map(new BookMapper(), Encoders.bean(Book.class));
        System.out.println("*** 将数据帧转换为书籍数据集：");
        bookDs.show(5, 17);
        bookDs.printSchema();

        Dataset<Row> df2 = bookDs.toDF();
        df2 = df2.withColumn(
                "releaseDateAsString",
                concat( expr("releaseDate.year + 1900"), lit("-"),
                        expr("releaseDate.month + 1"), lit("-"),
                        df2.col("releaseDate.date")));

        df2 = df2.withColumn(
                        "releaseDateAsDate",
                        to_date(df2.col("releaseDateAsString"), "yyyy-MM-dd"))
                .drop("releaseDateAsString");
        System.out.println("*** 将书籍数据集转换回数据帧：");
        df2.show(5, 13);
        df2.printSchema();
    }
}