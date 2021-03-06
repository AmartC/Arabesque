package io.arabesque.computation

import java.io._
import java.util.concurrent.{ExecutorService, Executors}

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.odag.domain.DomainEntry
import io.arabesque.odag._
import io.arabesque.odag.BasicODAGStash.EfficientReader
import io.arabesque.pattern.Pattern
import io.arabesque.utils.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, SequenceFile, Writable}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}
import scala.reflect.ClassTag

/**
 * Underlying engine that runs Arabesque workers in Spark.
 * Each instance of this engine corresponds to a partition in Spark computation
 * model. Instances' lifetimes refer also to one superstep of computation due
 * RDD's immutability.
 */
case class ODAGEngineSP [E <: Embedding](
    partitionId: Int,
    superstep: Int,
    accums: Map[String,Accumulator[_]],
    // TODO do not broadcast if user's code does not requires it
    previousAggregationsBc: Broadcast[_])
  extends ODAGEngine[E,SinglePatternODAG,SinglePatternODAGStash,ODAGEngineSP[E]] {

  // stashes
  nextEmbeddingStash = new SinglePatternODAGStash
  
  /**
   * Returns a new execution engine from this with the aggregations/computation
   * variables updated (immutability)
   *
   * @param aggregationsBc broadcast variable with aggregations
   * @return the new execution engine, ready for flushing
   */
  def withNewAggregations(aggregationsBc: Broadcast[_])
      : ODAGEngineSP[E] = {
    
    // we first get a copy of the this execution engine, with previous
    // aggregations updated
    val execEngine = this.copy [E] (
      previousAggregationsBc = aggregationsBc,
      accums = accums)

    // set next stash with odags
    execEngine.nextEmbeddingStash = nextEmbeddingStash
    
    execEngine
  }

  override def flush: Iterator[(_,_)] = configuration.getOdagFlushMethod match {
    case SparkConfiguration.FLUSH_BY_PATTERN => flushByPattern
    case SparkConfiguration.FLUSH_BY_ENTRIES => flushByEntries
    case SparkConfiguration.FLUSH_BY_PARTS =>   flushByParts
  }

  /**
   * Naively flushes outbound odags.
   * We assume that this execEngine is ready to
   * do *aggregationFilter*, i.e., this execution engine was generated by
   * [[withNewAggregations]].
   *
   * @return iterator of pairs of (pattern, odag)
   */
  private def flushByPattern: Iterator[(Pattern,SinglePatternODAG)]  = {
    // consume content in *nextEmbeddingStash*
    for (odag <- nextEmbeddingStash.getEzips().iterator
         if computation.aggregationFilter (odag.getPattern)
           )
      yield (odag.getPattern, odag)
  }

  /** 
   * Flushes outbound odags in parts, i.e., with single domain entries per odag
   * We assume that this execEngine is ready to
   * do *aggregationFilter*, i.e., this execution engine was generated by
   * [[withNewAggregations]].
   *
   *  @return iterator of pairs of ((pattern,domainId,wordId), odag_with_one_entry)
   */
  private def flushByEntries: Iterator[((Pattern,Int,Int), SinglePatternODAG)] = {

    /**
     * Iterator that split a big BasicODAG into small ODAGs containing only one entry
     * of the original. Thus, keyed by (pattern, domainId, wordId)
     */
    class ODAGPartsIterator(odag: SinglePatternODAG) extends Iterator[((Pattern,Int,Int),SinglePatternODAG)] {

      val domainIterator = odag.getStorage().getDomainEntries().iterator
      var domainId = -1
      var currEntriesIterator: Option[Iterator[(Integer,DomainEntry)]] = None

      val reusableOdag = new SinglePatternODAG(odag.getPattern(), odag.getNumberOfDomains())

      @scala.annotation.tailrec
      private def hasNextRec: Boolean = currEntriesIterator match {
        case None =>
          domainIterator.hasNext
        case Some(entriesIterator) if entriesIterator.isEmpty =>
          currEntriesIterator = None
          hasNextRec
        case Some(entriesIterator) =>
          entriesIterator.hasNext
      }

      override def hasNext = hasNextRec

      @scala.annotation.tailrec
      private def nextRec: ((Pattern,Int,Int),SinglePatternODAG) = currEntriesIterator match {

        case None => // set next domain and recursive call
          currEntriesIterator = Some(domainIterator.next.iterator)
          domainId += 1
          nextRec

        case Some(entriesIterator) => // format domain entry as new BasicODAG
          val newOdag = new SinglePatternODAG(odag.getPattern(), odag.getNumberOfDomains())
          val (wordId, entry) = entriesIterator.next
          val domainEntries = newOdag.getStorage().getDomainEntries()

          domainEntries.get (domainId).put (wordId, entry)

          ((newOdag.getPattern(),domainId,wordId.intValue), newOdag)

      }

      override def next = nextRec
    }

    // filter and flush
    nextEmbeddingStash.getEzips.iterator.
      filter (odag => computation.aggregationFilter (odag.getPattern)).
      flatMap (new ODAGPartsIterator(_))
  }

  /**
   * Flushes outbound odags by chunks of bytes
   * We assume that this execEngine is ready to
   * do *aggregationFilter*, i.e., this execution engine was generated by
   * [[withNewAggregations]].
   *
   * @return iterator of pairs ((pattern,partId), bytes)
   */
  private def flushByParts: Iterator[((Pattern,Int),Array[Byte])] = {

    val numPartitions = getNumberPartitions()
    val outputs = Array.fill[ByteArrayOutputStream](numPartitions)(new ByteArrayOutputStream())
    def createDataOutput(output: OutputStream): DataOutput = new DataOutputStream(output)
    val dataOutputs = outputs.map (output => createDataOutput(output))
    val hasContent = new Array[Boolean](numPartitions)

    nextEmbeddingStash.getEzips().iterator.
      filter (odag => computation.aggregationFilter(odag.getPattern)).
      flatMap { odag =>

        // reset aux structures
        var i = 0
        while (i < numPartitions) {
          outputs(i).reset
          hasContent(i) = false
          i += 1
        }
        
        // this method writes odag content into DataOutputs in parts
        odag.writeInParts (dataOutputs, hasContent)

        // attach to each byte array the corresponding key, i.e., (pattern, partId)
        for (partId <- 0 until numPartitions if hasContent(partId))
          yield ((odag.getPattern(), partId), outputs(partId).toByteArray)
      }
  }
  
}
