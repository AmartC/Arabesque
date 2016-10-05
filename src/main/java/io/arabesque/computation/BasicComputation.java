package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.function.IntConsumer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public abstract class BasicComputation<E extends Embedding> implements Computation<E>, java.io.Serializable {
    private static final Logger LOG = Logger.getLogger(BasicComputation.class);

    private boolean outputEnabled;

    protected CommonExecutionEngine<E> underlyingExecutionEngine;
    protected MainGraph mainGraph;
    private IntConsumer modifyConsumer;
    protected long numChildrenEvaluated = 0;
    protected E currentEmbedding;

    @Override
    public final void setUnderlyingExecutionEngine(CommonExecutionEngine<E> underlyingExecutionEngine) {
        this.underlyingExecutionEngine = underlyingExecutionEngine;
    }

    public MainGraph getMainGraph() {
        return mainGraph;
    }

    //public Configuration getConfiguration() {
    //    return configuration;
    //}

    @Override
    public void init() {
        modifyConsumer = new IntConsumer() {
            @Override
            public void accept(int wordId) {
                assert wordId >= 0;
                doModifyFilter(wordId);
            }
        };

        mainGraph = Configuration.get().getMainGraph();
        numChildrenEvaluated = 0;

        outputEnabled = Configuration.get().isOutputActive();
    }

    @Override
    public void initAggregations() {
        // Empty by default
    }

    @Override
    public void modify(E embedding) {
        currentEmbedding = embedding;
        if (getStep() > 0) {
            if (!aggregationFilter(embedding)) {
                return;
            }
            aggregationProcess(embedding);
        }

        //System.out.println("Current emb." + embedding.getWords());
        IntCollection possibleModifications = getPossibleModifications(embedding);
        //System.out.println("Possible Modif." + possibleModifications);

        if (possibleModifications != null && getStep() > 0) {
            filter(embedding, possibleModifications);
        }

        if (possibleModifications == null || possibleModifications.isEmpty()) {
            handleNoModifications(embedding);
            return;
        }

        possibleModifications.forEach(modifyConsumer);
    }

    protected void doModifyFilter(int wordId) {
        if (filter(currentEmbedding, wordId)) {
            currentEmbedding.addWord(wordId);

            if (filter(currentEmbedding)) {
                if (shouldModify(currentEmbedding)) {
                    underlyingExecutionEngine.processModification(currentEmbedding);
                }

                numChildrenEvaluated++;
                process(currentEmbedding);
            }
            currentEmbedding.removeLastWord();
        }
    }

    @Override
    public void handleNoModifications(E embedding) {
        // Empty by default
    }

    public IntCollection getPossibleExtensions(E embedding) {
        if (getStep() > 0 || embedding.getNumWords() > 0) {
            IntCollection extensions = embedding.getExtensibleWordIds();
            return extensions;
        } else {
            return getInitialExtensions();
        }
    }

    public IntCollection getPossibleContractions(E embedding) {
        return embedding.getContractibleWordIds();
    }

    protected IntCollection getPossibleModifications(E embedding) {
        return getPossibleExtensions(embedding);
    }

    protected IntArrayList getInitialExtensions() {
        int totalNumWords = getInitialNumWords();
        int numPartitions = getNumberPartitions();
        int myPartitionId = getPartitionId();
        int numWordsPerPartition = Math.max(totalNumWords / numPartitions, 1);
        int startMyWordRange = myPartitionId * numWordsPerPartition;
        int endMyWordRange = startMyWordRange + numWordsPerPartition;

        // If we are the last partition or our range end goes over the total number
        // of vertices, set the range end to the total number of vertices.
        if (myPartitionId == numPartitions - 1 || endMyWordRange > totalNumWords) {
            endMyWordRange = totalNumWords;
        }

        IntArrayList initialExtensions = new IntArrayList();
        for (int i = startMyWordRange; i < endMyWordRange; ++i) {
            initialExtensions.add(i);
        }

        return initialExtensions;
    }

    protected abstract int getInitialNumWords();

    @Override
    public boolean shouldModify(E embedding) {
        return true;
    }

    @Override
    public void filter(E existingEmbedding, IntCollection modificationPoints) {
        // Do nothing by default
    }

    @Override
    public boolean filter(E existingEmbedding, int newWord) {
        return existingEmbedding.isCanonicalEmbeddingWithWord(newWord);
    }

    @Override
    public <K extends Writable, V extends Writable> AggregationStorage<K, V> readAggregation(String name) {
        return underlyingExecutionEngine.getAggregatedValue(name);
    }

    @Override
    public <K extends Writable, V extends Writable> void map(String name, K key, V value) {
        underlyingExecutionEngine.map(name, key, value);
    }

    @Override
    public int getPartitionId() {
        return underlyingExecutionEngine.getPartitionId();
    }

    @Override
    public int getNumberPartitions() {
        return underlyingExecutionEngine.getNumberPartitions();
    }

    @Override
    public final int getStep() {
        // When we achieve steps that reach long values, the universe
        // will probably have ended anyway
        // ... that's true, doesn't matter
        return (int) underlyingExecutionEngine.getSuperstep();
    }

    @Override
    public boolean filter(E newEmbedding) {
        return true;
    }

    @Override
    public boolean aggregationFilter(E Embedding) {
        return true;
    }

    @Override
    public boolean aggregationFilter(Pattern pattern) {
        return true;
    }

    @Override
    public void aggregationProcess(E embedding) {
        // Empty by default
    }

    @Override
    public void finish() {
        LongWritable longWritable = new LongWritable();

        LOG.info("Num children evaluated: " + numChildrenEvaluated);
        //longWritable.set(numChildrenEvaluated);
        //underlyingExecutionEngine.aggregate(MasterExecutionEngine.AGG_CHILDREN_EVALUATED, longWritable);
    }

    @Override
    public void output(Embedding embedding) {
        if (outputEnabled) {
            underlyingExecutionEngine.output(embedding);
        }
    }

    @Override
    public void output(String outputString) {
        underlyingExecutionEngine.output(outputString);
    }
}
