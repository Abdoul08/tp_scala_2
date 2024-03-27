package tpScala.commons

import org.apache.spark.sql.{SparkSession}

object sparkutils {

  def get_sparkSession(name : String): SparkSession = {
    val spark = SparkSession.builder
      .appName(name)
      .getOrCreate()
    spark
  }
}
