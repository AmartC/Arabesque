package io.arabesque.gmlib.sampling;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.EdgeInducedSFSamplingComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import io.arabesque.embedding.BasicEmbedding;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class MotifEdgeSFSamplingComputation extends EdgeInducedSFSamplingComputation<EdgeInducedEmbedding> {
    public static final String AGG_MOTIFS = "motifs";
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final String MAXSTEP = "arabesque.motif.maxstep";
    private static final String AGGSTEP = "arabesque.motif.aggstep";
    private static final int MAXSIZE_DEFAULT = 4;
    private static final int MAXTSTEP_DEFAULT = 10;
    private static final int AGGSTEP_DEFAULT = 5;

    EdgeInducedEmbedding currentEmbedding;

    private static LongWritable reusableLongWritableUnit = new LongWritable(1);

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
        conf.registerAggregation(AGG_MOTIFS, conf.getPatternClass(), LongWritable.class, true, new LongSumReduction());
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
            map(AGG_MOTIFS, embedding.getPattern(), reusableLongWritableUnit);
        }
    }

    /*
    public Collection<Double> getUnbiasedFactors(EdgeInducedEmbedding embedding) {
        currentEmbedding = embedding;

        HashMap<IntArrayList, Double> groupsSize = new HashMap<>();
        HashMap<IntArrayList, Double> groupsOut = new HashMap<>();

        boolean previousOk = false;

        while (!currentEmbedding.isAutomorphic(embedding)) {
            IntCollection exts = currentEmbedding.getExtensibleWordIds();
            int wordId = nextModification(exts.toIntArray(),exts.size());
            int rmWordId = processChange(currentEmbedding,wordId);

            //check if the new emb. belongs to same group
            IntArrayList shared = embedding.getSharedWordIds(currentEmbedding);

            if (shared.size() == embedding.getNumWords()) {
                //identical embeddings then return
                break;
            }
            else if (shared.size() < embedding.getNumWords()-1) {
                //restore the previous embedding
                currentEmbedding.addWord(rmWordId);
                currentEmbedding.removeWord(wordId);

                shared = embedding.getSharedWordIds(currentEmbedding);
                if (!groupsOut.containsKey(shared)) {
                    groupsOut.put(shared, 1.);
                } else {
                    double b = groupsSize.get(shared);
                    groupsOut.put(shared, b+1.);
                }
            }
            else {
               if (!groupsSize.containsKey(shared)) {
                   groupsSize.put(shared, 1.);
               } else {
                   double b = groupsSize.get(shared);
                   groupsSize.put(shared, b+1.);
               }
            }
        }
        return groupsOut.values();
    }

    private int processChange(EdgeInducedEmbedding embedding, int wordId) {
        embedding.addWord(wordId);

        //choose edge to replace
        IntCollection contractions = currentEmbedding.getContractibleWordIds();
        System.out.println("Valid contractions: " + contractions);
        contractions.removeInt(wordId);
        int[] contractionsArray = contractions.toIntArray();
        int i = r.nextInt(contractions.size());
        //System.out.println("Selected pos: " + i);
        currentEmbedding.removeWord(contractionsArray[i]);

        return contractionsArray[i];
    }*/
}
