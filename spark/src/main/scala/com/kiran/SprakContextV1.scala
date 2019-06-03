package com.kiran

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object SprakContext {

	def main(args: Array[String]) : Unit = {

	    //Spark 1.x style
			val conf = new SparkConf();
			conf.setAppName("First Spark Scala App")
			conf.setMaster("local")

			val sc = new SparkContext(conf)

			val rdd = sc.parallelize(Array(1,2,3,4,5))
			

			println("Num of elem in RDD ", rdd.count())
			rdd.foreach(println)
			
			val fileRdd = sc.textFile("/Users/kiran/Downloads/FL_insurance_sample.csv", 2)
			val header = fileRdd.first
			
			print("Files count", fileRdd.count)
			print("Files count", fileRdd.count())
			fileRdd.take(10).filter(_ != header).foreach(println)
	}
}