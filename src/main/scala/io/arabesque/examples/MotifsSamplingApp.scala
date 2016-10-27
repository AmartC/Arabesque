package io.arabesque.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import io.arabesque._
import io.arabesque.conf.Configuration._
import io.arabesque.conf.SparkConfiguration._

object MotifsSamplingApp {
  def main(args: Array[String]) {
    val input = args(0)
    val sizeLower = args(1).toInt
    val sizeUpper = args(2).toInt
    val conf = new SparkConf().
      setAppName("Motif Sampling")
    val sc = new SparkContext (conf)
    val arab = new ArabesqueContext (sc)
    val arabGraph = arab.textFile (input)
    val motifsRes = arabGraph.motifsSampling (sizeLower, sizeUpper, 100, 10000).
      set ("log_level", "debug").
      set ("comm_strategy", COMM_EMBEDDING).
      set ("num_partitions", 16)
    val embeddings = motifsRes.embeddings
    sc.stop
    arab.stop
  }
}
