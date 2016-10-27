package io.arabesque.computation;

import io.arabesque.embedding.Embedding;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public interface CommonExecutionEngine<O extends Embedding> {

    void processModification(O expansion);

    <A extends Writable> A getAggregatedValue(String name);

    <K extends Writable, V extends Writable> void map(String name, K key, V value);
    
    int getPartitionId();

    int getNumberPartitions();

    long getSuperstep();

    void aggregate(String name, LongWritable value);
    
    void output(String outputString);

    void output(Embedding embedding);

}
