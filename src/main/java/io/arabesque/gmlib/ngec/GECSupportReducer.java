package io.arabesque.gmlib.ngec;

import io.arabesque.aggregation.reductions.ReductionFunction;

public class GECSupportReducer
   extends ReductionFunction<GECSupport> {
    @Override
    public GECSupport reduce(GECSupport k1, GECSupport k2) {
        k1.aggregate(k2);

        return k1;
    }
}
