package io.arabesque.computation

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.odag.ODAG
import io.arabesque.aggregation.AggregationStorage

import org.apache.hadoop.io.{Writable, NullWritable}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map

abstract class SparkMasterEngine(config: SparkConfiguration[_ <: Embedding])
    extends CommonMasterExecutionEngine with Logging {
  
  var sc: SparkContext = _

  def init(): Unit
  def compute(): Unit
  def finalizeComputation(): Unit

  /**
   * Merges or replaces the aggregations for the next superstep. We can have one
   * of the following scenarios:
   * (1) In any superstep we are interested in all aggregations seen so far.
   *     Thus, the aggregations are incrementally composed.
   * (2) In any superstep we are interested only in the previous
   *     aggregations. Thus, we discard the old aggregations and replace it with
   *     the new aggregations for the next superstep.
   *
   *  @param aggregations current aggregations
   *  @param previousAggregations aggregations found in the superstep that just
   *  finished
   *
   *  @return the new choice for aggregations obtained by composing or replacing
   */
  def mergeOrReplaceAggregations (
      aggregations: Map[String,AggregationStorage[_ <: Writable, _ <: Writable]],
      previousAggregations: Map[String,AggregationStorage[_ <: Writable, _ <: Writable]])
  : Map[String,AggregationStorage[_ <: Writable,_ <: Writable]] = if (config.isAggregationIncremental) {
    // we compose all entries
    println("BLA mergeOR")
    previousAggregations.foreach {case (k,v) => aggregations.update (k,v)}
    //println("SIZEAGG new: " + aggregations("sampling").getNumberMappings)
    aggregations
  } else {
    // we replace with new entries
    //println("SIZEAGG previous: " + previousAggregations("sampling").getNumberMappings)
    previousAggregations
  }

  /**
   * Functions that retrieve the results of this computation.
   * Current fields:
   *  - Odags of each superstep. ATENTION: always empty here because of the
   *  execution engine being used (i.e. embedding caches)
   *  - Embeddings if the output is enabled. Our choice is to read the results
   *  produced by the supersteps from external storage. We avoid memory issues
   *  by not keeping all the embeddings in memory.
   */
  def getOdags: RDD[ODAG] = {
    sc.makeRDD (Seq.empty[ODAG])
  }
  def getEmbeddings: RDD[ResultEmbedding] = {

    val embeddPath = s"${config.getOutputPath}"
    val fs = FileSystem.get (sc.hadoopConfiguration)

    if (config.isOutputActive && fs.exists (new Path (embeddPath))) {
      logInfo (s"Reading embedding words from: ${config.getOutputPath}")
      //sc.textFile (s"${embeddPath}/*").map (ResultEmbedding(_))

      // we must decide at runtime the concrete Writable to be used
      val resEmbeddingClass = if (config.getEmbeddingClass == classOf[EdgeInducedEmbedding])
        classOf[EEmbedding]
      else if (config.getEmbeddingClass == classOf[VertexInducedEmbedding])
        classOf[VEmbedding]
      else
        classOf[ResultEmbedding] // not allowed, will crash and should not happen

      sc.sequenceFile (s"${embeddPath}/*", classOf[NullWritable], resEmbeddingClass).
        map {
          case (_,e: EEmbedding) => e.copy()
          case (_,e: VEmbedding) => e.copy()
        }. // writables are reused, workaround on that
        asInstanceOf[RDD[ResultEmbedding]]
    } else {
      sc.emptyRDD[ResultEmbedding]
    }
  }
}

object SparkMasterEngine {
  import Configuration._
  import SparkConfiguration._

  def apply(sc: SparkContext, config: SparkConfiguration[_ <: Embedding])
    : SparkMasterEngine =
      config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT) match {
    case COMM_ODAG =>
      new SparkODAGMasterEngine (sc, config)
    case COMM_EMBEDDING =>
      new SparkEmbeddingMasterEngine (sc, config)
  }

  def apply(confs: Map[String,Any])
    : SparkMasterEngine = confs.get ("comm_strategy") match {
    case Some(COMM_ODAG) =>
      new SparkODAGMasterEngine (confs)
    case Some(COMM_EMBEDDING) =>
      new SparkEmbeddingMasterEngine (confs)
    case None =>
      confs.update ("comm_strategy", CONF_COMM_STRATEGY_DEFAULT)
      apply (confs)
    case Some(invalid) =>
      throw new RuntimeException(s"Communication Strategy is invalid: ${invalid}")
  }
}
