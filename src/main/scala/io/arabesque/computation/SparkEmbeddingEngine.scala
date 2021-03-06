package io.arabesque.computation

import java.io.OutputStreamWriter

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.Configuration
import io.arabesque.embedding._
import io.arabesque.utils.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Writable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
 * Spark engine that works with raw embedding representation
 */
case class SparkEmbeddingEngine[O <: Embedding](
    partitionId: Int,
    superstep: Int,
    accums: Map[String,Accumulator[_]],
    // TODO do not broadcast if user's code does not requires it
    previousAggregationsBc: Broadcast[_])
  extends SparkEngine[O] {

  // embedding caches
  var embeddingCaches: Array[LZ4ObjectCache] = _
  var currentCache: LZ4ObjectCache = _

  // round robin id
  var _nextGlobalId: Long = _
  def nextGlobalId: Long = {
    val id = _nextGlobalId
    _nextGlobalId += 1
    id
  }

  override def init() = {
    if (configuration.getEmbeddingClass() == null)
      configuration.setEmbeddingClass (computation.getEmbeddingClass())

    // embedding caches
    embeddingCaches = Array.fill (getNumberPartitions) (new LZ4ObjectCache)
    currentCache = null

    // global round-robin id
    val countersPerPartition = Long.MaxValue / getNumberPartitions
    _nextGlobalId = countersPerPartition * partitionId

    // accumulators
    numEmbeddingsProcessed = 0
    numEmbeddingsGenerated = 0
    numEmbeddingsOutput = 0
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    // make sure we close writers
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (embeddingWriterOpt.isDefined) embeddingWriterOpt.get.close
  }

  /**
   * It does the computation of this module, i.e., expand/compute
   *
   * @param inboundCaches
   */
  def compute(inboundCaches: Iterator[LZ4ObjectCache]) = {
    expansionCompute (inboundCaches)
    flushStatsAccumulators
  }

  /**
   * Iterates over embedding caches and call expansion/compute procedures on them.
   * It also bootstraps the cycle by requesting empty embedding from
   * configuration and expanding them.
   *
   * @param inboundCaches iterator of embedding caches
   */
  private def expansionCompute(inboundCaches: Iterator[LZ4ObjectCache]): Unit = {
    if (superstep == 0) { // bootstrap

      val initialEmbedd: O = configuration.createEmbedding()
      computation.expand (initialEmbedd)

    } else {
      var hasNext = true
      while (hasNext) getNextInboundEmbedding (inboundCaches) match {
        case None =>
          hasNext = false

        case Some(embedding) =>
          internalCompute (embedding)
          numEmbeddingsProcessed += 1
      }
    }
  }

  /**
   * Calls computation to expand an embedding
   *
   * @param embedding embedding to be expanded
   */
  def internalCompute(embedding: O) = computation.expand (embedding)

  /**
   * Reads next embedding from previous caches
   *
   * @param remainingCaches 
   * @return some embedding or none
   */
  @scala.annotation.tailrec
  private def getNextInboundEmbedding(
      remainingCaches: Iterator[LZ4ObjectCache]): Option[O] = {
    if (currentCache == null) {
      if (remainingCaches.hasNext) {
        currentCache = remainingCaches.next
        currentCache.prepareForIteration
        getNextInboundEmbedding (remainingCaches)
      } else None
    } else {
      if (currentCache.hasNext) {
        val embedding = currentCache.next.asInstanceOf[O]
        if (computation.aggregationFilter(embedding.getPattern))
          Some(embedding)
        else
          getNextInboundEmbedding (remainingCaches)
      } else {
        currentCache = null
        getNextInboundEmbedding (remainingCaches)
      }
    }
  }

  /**
   * Called whenever an embedding survives the expand/filter process and must be
   * carried on to the next superstep
   *
   * @param embedding embedding that must be processed
   */
  def addOutboundEmbedding(embedding: O) = processExpansion (embedding)

  /**
   * Adds an expansion (embedding) to the outbound odags.
   *
   * @param expansion embedding to be added to the stash of outbound odags
   */
  override def processExpansion(expansion: O) = {
    val destId = (nextGlobalId % getNumberPartitions).toInt
    val cache = embeddingCaches(destId)
    cache.addObject (expansion)
    numEmbeddingsGenerated += 1
  }

  def flush: Iterator[(Int,LZ4ObjectCache)] = {
    if (numEmbeddingsGenerated > 0)
      (0 until embeddingCaches.size).iterator.
        map (i => (i, embeddingCaches(i)))
    else Iterator.empty
  }
}
