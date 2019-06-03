package com.kiran

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object SprakDataFrameV2 {


	def main(args: Array[String]) : Unit = {


			val sparkSession = SparkSession
					.builder()
					.appName("Spark DataFrame V2")
					.master("local")
					.getOrCreate()


					val sqlContext = sparkSession.sqlContext

					val rdd = sparkSession.sparkContext.parallelize(Array(6,7,8,9,10))

					val schema = StructType(
							StructField("Numbers", IntegerType, false) :: Nil
							)

					val rowRDD = rdd.map(line => Row(line))

					val df = 	sqlContext.createDataFrame(rowRDD, schema)

					df.show()



	}
}