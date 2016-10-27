package io.arabesque.gmlib.ngec;

import io.arabesque.computation.EdgeInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;

public class GECComputation extends EdgeInducedComputation<EdgeInducedEmbedding> {
    public static final String AGG_MOTIFS = "motifs";
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final int MAXSIZE_DEFAULT = 4;


    private GECSupport reusableGECSupport;
    private int maxsize;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
        reusableGECSupport = new GECSupport();
    }

    @Override
    public void initAggregations() {
        super.initAggregations();
        Configuration conf = Configuration.get();
        conf.registerAggregation(AGG_MOTIFS, conf.getPatternClass(), GECSupport.class, true, new GECSupportReducer());
    }

    @Override
    public boolean shouldModify(EdgeInducedEmbedding embedding) {
        return embedding.getNumEdges() < maxsize;
    }

    @Override
    public void process(EdgeInducedEmbedding embedding) {
        if (embedding.getNumWords() == maxsize) {
            output(embedding);
            reusableGECSupport.setFromEmbedding(embedding);
            map(AGG_MOTIFS, embedding.getPattern(), reusableGECSupport);
        }
    }
}
