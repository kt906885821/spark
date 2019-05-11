package com.kt.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountJava {
    public static final Logger logger = LoggerFactory.getLogger(WordCountJava.class);

    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setAppName("wordcount")
                .setMaster("local[2]")
                ;
        //创建context对象
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        //读取本地文件数据
        JavaRDD<String> javaRDD = context.textFile("D:\\Spark\\word.txt");
//        javaRDD.checkpoint("");
        JavaRDD<String> flatMap = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });
        JavaPairRDD<String, Integer> javaPairRDD = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceByKey = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                //相同的单词累加
                return integer + integer2;
            }
        });
        JavaPairRDD<Integer, String> sortbyRdd = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                //反转顺序

                return new Tuple2<>(stringIntegerTuple2._2,stringIntegerTuple2._1);
            }
        });

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sortbyRdd.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {

                return new Tuple2<String, Integer>(integerStringTuple2._2,integerStringTuple2._1);
            }
        });
        logger.info(String.valueOf(stringIntegerJavaPairRDD.collect()));
        context.close();


    }




}
