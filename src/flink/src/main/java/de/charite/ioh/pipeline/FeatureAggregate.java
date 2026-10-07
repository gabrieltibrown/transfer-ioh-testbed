package de.charite.ioh.pipeline;

import org.apache.flink.api.common.functions.AggregateFunction;

/** Incremental aggregation so raw wave packets never sit in window state. */
public class FeatureAggregate implements AggregateFunction<Measurement, FeatureAcc, FeatureAcc> {
    private static final long serialVersionUID = 1L;

    @Override
    public FeatureAcc createAccumulator() {
        return new FeatureAcc();
    }

    @Override
    public FeatureAcc add(Measurement m, FeatureAcc acc) {
        acc.add(m);
        return acc;
    }

    @Override
    public FeatureAcc getResult(FeatureAcc acc) {
        return acc;
    }

    @Override
    public FeatureAcc merge(FeatureAcc a, FeatureAcc b) {
        return a.merge(b);
    }
}
