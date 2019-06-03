package com.kiran

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

object SparkDataFrame {

	def main(args: Array[String]) :Unit = {

			val sparkConf = new SparkConf().setAppName("Data frame V1").setMaster("local")

					val sparkContext = new SparkContext(sparkConf)

					val sqlContext = new SQLContext(sparkContext)

					val rdd = sparkContext.parallelize(Array(1,2,3,4,5))

					val schema = StructType(
							StructField("Numbers", IntegerType, false) :: Nil
							)

					val rowRDD = rdd.map(line => Row(line))

					val df = 	sqlContext.createDataFrame(rowRDD, schema)

					df.show()
	}

}