package io.arabesque.gmlib.sampling;

import io.arabesque.aggregation.reductions.DoubleSumReduction;
import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.EdgeInducedSFSamplingComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.HashMap;

public class MotifEdgeSFSamplingComputation extends EdgeInducedSFSamplingComputation<EdgeInducedEmbedding> {
    public static final String AGG_SAMPLING = "sampling";
    private static final String MAXSIZE = "arabesque.sampling.maxsize";
    private static final String MAXSTEP = "arabesque.sampling.maxstep";
    private static final String AGGSTEP = "arabesque.sampling.aggstep";
    private static final int MAXSIZE_DEFAULT = 4;
    private static final int MAXTSTEP_DEFAULT = 10;
    private static final int AGGSTEP_DEFAULT = 5;

    private static DoubleWritable reusableDoubleWritableUnit = new DoubleWritable(1);
    //private static LongWritable reusableLongWritableUnit = new LongWritable(1);

    private int maxsize;
    private int maxstep;
    private int aggstep;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
        maxstep = Configuration.get().getInteger(MAXSTEP, MAXTSTEP_DEFAULT);
        aggstep = Configuration.get().getInteger(AGGSTEP, AGGSTEP_DEFAULT);
    }

    @Override
    public void initAggregations() {
        super.initAggregations();
        Configuration conf = Configuration.get();
        conf.registerAggregation(AGG_SAMPLING, conf.getPatternClass(), DoubleWritable.class, true, new DoubleSumReduction());
        //conf.registerAggregation(AGG_SAMPLING, conf.getPatternClass(), LongWritable.class, false, new LongSumReduction());
    }

    public boolean shouldModify(EdgeInducedEmbedding embedding) {
        return getStep() < maxstep;
    }

    @Override
    public void process(EdgeInducedEmbedding embedding) {
        if (getStep() >= aggstep &&
                embedding.getNumWords() == maxsize) {
            output(embedding);
            System.out.println("ewords: " + embedding.getWords());

            reusableDoubleWritableUnit.set(getGroupFactors(embedding));
            map(AGG_SAMPLING, embedding.getPattern(), reusableDoubleWritableUnit);
            //map(AGG_SAMPLING, embedding.getPattern(), reusableLongWritableUnit);
        }
    }

    public double getGroupFactors(EdgeInducedEmbedding embedding) {
        EdgeInducedEmbedding embeddingCopy = new EdgeInducedEmbedding();
        embeddingCopy.copy(embedding);

        //System.out.println("get Groups for embedding:");
        //System.out.println("edges:" + embeddingCopy.getEdges());
        //System.out.println("vertices:" + embeddingCopy.getVertices());
        //System.out.println("addedWithWords:" + embeddingCopy.getNumWordsAddedWithWord());

        HashMap<IntArrayList, Integer> groups = new HashMap<>();

        int k = 0;
        do {
            IntCollection exts = embeddingCopy.getExtensibleWordIds();
            int wordId = nextModification(embeddingCopy, exts.toIntArray(), exts.size());
            int rmWordId = processChange(embeddingCopy, wordId);

            //check if the new emb. belongs to same group
            IntArrayList shared = embedding.getSharedWordIds(embeddingCopy);

            if (shared.size() == embedding.getNumWords()) {
                break; //identical embeddings then return
            } else if (shared.size() < embedding.getNumWords() - 1) {
                //restore the previous embedding
                embeddingCopy.addWord(rmWordId);
                embeddingCopy.removeWord(wordId);
            } else {
                int embeddingDegree = getNumberOfEmbeddingsNeighbors(embeddingCopy);
                if (!groups.containsKey(shared)) {
                    groups.put(shared, embeddingDegree);
                } else {
                    //System.out.println("COLISION: " + shared);
                    int d = groups.get(shared);
                    groups.put(shared, d + embeddingDegree);
                }
            }
            k++;
        } while (!embeddingCopy.isAutomorphic(embedding));

        double factor = 0;
        for (Integer i : groups.values()) {
            factor += 1 / i.doubleValue();
        }

        int embeddingDegree = getNumberOfEmbeddingsNeighbors(embeddingCopy);
        if (factor == 0) factor = embeddingDegree;
        else factor *= embeddingDegree;
        //System.out.println("RW Tour size: " + k);
        return factor;
    }

    private int processChange(EdgeInducedEmbedding embedding, int wordId) {
        embedding.addWord(wordId);

        //choose edge to replace
        IntCollection contractions = embedding.getContractibleWordIds();
        contractions.removeInt(wordId);
        int[] contractionsArray = contractions.toIntArray();
        int i = r.nextInt(contractions.size());
        //System.out.println("Selected pos: " + i);
        embedding.removeWord(contractionsArray[i]);

        return contractionsArray[i];
    }
}
