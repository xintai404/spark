package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */

        SparkConf conf = new SparkConf().setAppName("airportsNotInUsa").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> airportPairRDD = lines.mapToPair(getAirportNameAndCountryNamePair());

        JavaPairRDD<String, String> airportsNotInUSA = airportPairRDD.filter(keyValue -> !keyValue._2().equals("\"United States\""));

        airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
    }

    private static PairFunction<String, String, String> getAirportNameAndCountryNamePair(){
        return (PairFunction<String, String, String>) s -> new Tuple2<>(s.split(Utils.COMMA_DELIMITER)[1],
                                                                        s.split(Utils.COMMA_DELIMITER)[3]);
    }
}
