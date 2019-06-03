package com.kiran

import org.apache.spark.sql.SparkSession

object SparkContextV2 {

	def main(args: Array[String]) : Unit = {

			val session = SparkSession
					.builder()
					.appName("Spark Context V2")
					.master("local")
					.getOrCreate()
			
			val rdd = session.sparkContext.textFile("/Users/kiran/Downloads/FL_insurance_sample.csv", 2)
			
			val header = rdd.first
			
			val csvRdd = rdd.take(10).filter(_ != header)
			
			csvRdd.foreach(println)
			
			val finalRDD = csvRdd.map(line => {
			    val cols = line.split(",")
			    print("cols ", cols(0))
			    Array(cols(0), cols(1)).mkString(",")
			})
			
			finalRDD.foreach(print)
			
		
			
			
			
			
			
			
			
	}

}