package io.arabesque.aggregation.reductions;

import org.apache.hadoop.io.DoubleWritable;

public class DoubleSumReduction extends ReductionFunction<DoubleWritable> {
    @Override
    public DoubleWritable reduce(DoubleWritable k1, DoubleWritable k2) {
        if (k1 != null && k2 != null) {
            k1.set(k1.get() + k2.get());
        }

        return k1;
    }
}
