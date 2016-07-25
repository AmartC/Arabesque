package io.arabesque.gmlib.motif;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.EdgeInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import org.apache.hadoop.io.LongWritable;

public class MotifEdgeComputation extends EdgeInducedComputation<EdgeInducedEmbedding> {
    public static final String AGG_MOTIFS = "motifs";
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final int MAXSIZE_DEFAULT = 4;

    private static LongWritable reusableLongWritableUnit = new LongWritable(1);

    private int maxsize;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
    }

    @Override
    public void initAggregations() {
        super.initAggregations();

        Configuration conf = Configuration.get();

        conf.registerAggregation(AGG_MOTIFS, conf.getPatternClass(), LongWritable.class, true, new LongSumReduction());
    }

    @Override
    public boolean shouldModify(EdgeInducedEmbedding embedding) {
        return embedding.getNumEdges() < maxsize;
    }

    @Override
    public void process(EdgeInducedEmbedding embedding) {
        if (embedding.getNumWords() == maxsize) {
            System.out.println("ewords: " + embedding.getWords());
            output(embedding);
            map(AGG_MOTIFS, embedding.getPattern(), reusableLongWritableUnit);
        }
    }
}
