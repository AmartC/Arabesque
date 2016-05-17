package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;
import net.openhft.koloboke.collect.IntCollection;
import org.apache.hadoop.io.Writable;

public interface Computation<E extends Embedding> {
    // {{{ Initialization and finish hooks
    void init();

    void initAggregations();

    void finish();
    // }}}

    // {{{ Filter-Process model
    boolean filter(E embedding);

    void process(E embedding);

    boolean aggregationFilter(E Embedding);
    boolean aggregationFilter(Pattern pattern);

    void aggregationProcess(E embedding);

    void handleNoModifications(E embedding);

    boolean shouldModify(E newEmbedding);
    // }}}

    // {{{ Other filter-hooks (performance/canonicality related)
    void filter(E existingEmbedding, IntCollection extensionPoints);

    boolean filter(E existingEmbedding, int newWord);
    // }}}

    // {{{ Output
    void output(String outputString);
    void output(Embedding embedding);
    // }}}

    // {{{ Aggregation-related stuff
    <K extends Writable, V extends Writable> AggregationStorage<K, V> readAggregation(String name);

    <K extends Writable, V extends Writable> void map(String name, K key, V value);

    // }}}

    // {{{ Misc
    int getStep();

    int getPartitionId();

    int getNumberPartitions();
    // }}}

    // {{{ Internal
    void setUnderlyingExecutionEngine(CommonExecutionEngine<E> executionEngine);
    void modify(E embedding);
    Class<? extends Embedding> getEmbeddingClass();
    // }}}
}
