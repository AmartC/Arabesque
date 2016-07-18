package io.arabesque.gmlib.motif.sampling;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.VertexInducedSamplingComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.pattern.Pattern;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import org.apache.hadoop.io.LongWritable;

public class MotifSamplingComputation extends VertexInducedSamplingComputation<VertexInducedEmbedding> {
    public static final String AGG_MOTIFS = "motifs";
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final String MAXSTEP = "arabesque.motif.maxstep";
    private static final String AGGSTEP = "arabesque.motif.aggstep";
    private static final int MAXSIZE_DEFAULT = 4;
    private static final int MAXTSTEP_DEFAULT = 10;
    private static final int AGGSTEP_DEFAULT = 5;


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

    public boolean shouldModify(VertexInducedEmbedding embedding) {
        return getStep() < maxstep;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        if (getStep() >= aggstep && 
              embedding.getNumWords() >= 3 && embedding.getNumWords() <= 5) {
           output(embedding);
            map(AGG_MOTIFS, embedding.getPattern(), reusableLongWritableUnit);
        }
    }
    
    @Override
    protected IntCollection getPossibleModifications(VertexInducedEmbedding embedding) {
       IntCollection possibleModifications;

       if (embedding.getNumWords() <= 3) {
          possibleModifications = getPossibleExtensions(embedding);
       } else if (embedding.getNumWords() == 4) {
          possibleModifications = getPossibleExtensions(embedding);
          possibleModifications.addAll(getPossibleContractions(embedding));
       } else {
          possibleModifications = getPossibleContractions(embedding);
       }

       return possibleModifications;
    }
}
