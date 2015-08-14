package com.epam.spark

// Spark
import org.apache.spark.{
SparkContext,
SparkConf
}
import SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
    sc
      .textFile(args(0))
      .flatMap(line => split(line))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)
      .map(_.swap)
      .saveAsTextFile(args(1))
  }

  private def split(line : String) : Array[String] = {
    line.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
