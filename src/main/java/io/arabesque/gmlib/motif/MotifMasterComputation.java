package io.arabesque.gmlib.motif;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.MasterComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.LongWritable;

public class MotifMasterComputation extends MasterComputation {

    private static final String MAXSTEP = "arabesque.motif.maxstep";
    private static final int MAXTSTEP_DEFAULT = 10;

    private int maxstep;

    @Override
    public void init() {
        maxstep = Configuration.get().getInteger(MAXSTEP, MAXTSTEP_DEFAULT);
    }

    @Override
    public void compute() {
        System.out.println("Sampling Master computing");

        AggregationStorage<Pattern, LongWritable> aggregationStorage =
                readAggregation(MotifEdgeComputation.AGG_MOTIFS);

        System.out.println("Aggregation Storage: " + aggregationStorage);

        if (aggregationStorage.getNumberMappings() > 0) {
            System.out.println("Patterns:");

            int i = 1;
            for (Pattern pattern : aggregationStorage.getKeys()) {
                System.out.println("P#" + i + ": " + pattern + ": " + aggregationStorage.getValue(pattern));
                ++i;
            }
        } else if (getStep() > 0) {
            System.out.println("Empty.");
        }
    }
}