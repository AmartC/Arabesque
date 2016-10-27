package io.arabesque.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import io.arabesque._
import io.arabesque.conf.Configuration._
import io.arabesque.conf.SparkConfiguration._

object MotifsApp {
  def main(args: Array[String]) {
    val input = args(0)
    val maxsize = args(1).toInt
    val conf = new SparkConf().
      setAppName("Motif")
    val sc = new SparkContext (conf)
    val arab = new ArabesqueContext (sc)
    val arabGraph = arab.textFile (input)
    val motifsRes = arabGraph.motifs (maxsize).
      set ("log_level", "debug").
      set ("comm_strategy", COMM_ODAG_SP).
      set ("num_partitions", 16)
    val embeddings = motifsRes.embeddings
    sc.stop
    arab.stop
  }
}
