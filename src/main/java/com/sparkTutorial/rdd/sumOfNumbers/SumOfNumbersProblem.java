package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("sunNums").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String>lines = sc.textFile("in/prime_nums.text");
        JavaRDD<String>nums = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
        JavaRDD<String>nonEmpty = nums.filter(num -> !num.isEmpty());

        JavaRDD<Integer>intNums = nonEmpty.map(num -> Integer.valueOf(num));


        System.out.println(intNums.reduce((x,y) -> x+y));
    }
}
