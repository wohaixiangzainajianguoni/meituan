package com.zqg.kakfautils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SmallTest {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("mysql");

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf).getOrCreate();

        Properties readConnProperties1 = new Properties();
        readConnProperties1.put("driver", "com.mysql.jdbc.Driver");
        readConnProperties1.put("user", "root");
        readConnProperties1.put("password", "111111");

        readConnProperties1.put("fetchsize", "3");

        Dataset<Row> jdbc = spark.read().jdbc(
                "jdbc:mysql://192.168.178.133:3306/bigdata",
                "ClickCount",
                readConnProperties1);

        jdbc.show();
        JavaRDD<Row> javaRDD = jdbc.javaRDD();


    }
}
