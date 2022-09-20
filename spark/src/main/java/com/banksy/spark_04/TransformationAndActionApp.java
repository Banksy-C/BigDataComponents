package com.banksy.spark_04;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.apache.spark.sql.functions.expr;

/**
 * @Author banksy
 * @Data 2022/9/19 8:49 PM
 * @Version 1.0
 **/
public class TransformationAndActionApp {
    @Test
    public static void main(String[] args) {
        TransformationAndActionApp app = new TransformationAndActionApp();
        String mode = "noop";
        if (args.length != 0) {
            mode = args[0];
        }
        app.start(mode);
    }

    private void start(String mode){
        /* 第1步：获取会话； */
        long t0 = System.currentTimeMillis();//设置计时器
        SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")//分析Catalyst的行为
                .master("local")
                .getOrCreate();
        long t1 = System.currentTimeMillis();//测量创建会话所花费的时间
        System.out.println("1. Creating a session ........... " + (t1 - t0));

        /* 第2步：从CSV文件中提取数据； */
        Dataset<Row> df = spark.read().format("csv").option("header", "true").load("spark/data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");
        Dataset<Row> initalDf = df;//创建要在副本中使用的参考数据帧
        long t2 = System.currentTimeMillis();//测量读取文件和创建数据帧所花费的时间
        System.out.println("2. Loading initial dataset ...... " + (t2 - t1));

        /* 第3步：将数据帧与它自身合并，并创建较大的数据集；*/
        for (int i = 0; i < 10; i++) {//循环操作，增大数据集
            df = df.union(initalDf);
        }
        long t3 = System.currentTimeMillis();//测量构建较大数据集所需的时间
        System.out.println("3. Building full dataset ........ " + (t3 - t2));

        /* 第4步：重命名列；*/
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");
        long t4 = System.currentTimeMillis();//测量数据清理所需的时间
        System.out.println("4. Clean-up ..................... " + (t4 - t3));

        /* 第5步：使用不同模式进行实际的数据转换；*/
        if (mode.compareToIgnoreCase("noop") != 0) {//如果模式为noop，则跳过所有数据转换；否则创建新列；
            df = df
                    .withColumn("avg", expr("(lcl+ucl)/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ucl2", df.col("ucl"));
            if (mode.compareToIgnoreCase("full") == 0) {//若模式为full，则删除新创建的列
                df = df
                        .drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }
        long t5 = System.currentTimeMillis();//测量数据转换所用的时间
        System.out.println("5. Transformations  ............. " + (t5 - t4));

        /* 第6步：调用操作；【Action】*/
        df.collect();//执行收集操作
        long t6 = System.currentTimeMillis();
        System.out.println("6. Final action ................. " + (t6 - t5));//测量完成操作所需的时间

        System.out.println("");
        System.out.println("# of records .................... " + df.count());

    }
}