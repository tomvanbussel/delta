package io.delta.connect

import java.util.concurrent.TimeUnit

import scala.io.StdIn

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService

object SimpleDeltaConnectService {
  private val stopCommand = "q"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    // scalastyle:off println
    println("Ready for client connections.")
    // scalastyle:on println
    while (true) {
      val code = StdIn.readLine()
      if (code == stopCommand) {
        // scalastyle:off println
        println("No more client connections.")
        // scalastyle:on println
        // Wait for 1 min for the server to stop
        SparkConnectService.stop(Some(1), Some(TimeUnit.MINUTES))
        sparkSession.close()
        sys.exit(0)
      }
    }
  }
}
