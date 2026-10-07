package de.charite.ioh.pipeline;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Attaches window bounds, watermark at fire, the firing wall time and the
 * partial flag. The first event time per key is kept in global (per-key,
 * cross-window) state; windows fire in order, so by the time the first window
 * fires the state holds the case's first event.
 */
public class FeatureWindowFunction
        extends ProcessWindowFunction<FeatureAcc, FeatureVector, String, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private static final ValueStateDescriptor<Long> FIRST_EVENT = new ValueStateDescriptor<>("firstEventMs", Long.class);

    @Override
    public void process(String key, Context ctx, Iterable<FeatureAcc> accs, Collector<FeatureVector> out)
            throws Exception {
        FeatureAcc acc = accs.iterator().next();
        ValueState<Long> firstState = ctx.globalState().getState(FIRST_EVENT);
        Long first = firstState.value();
        if (first == null || acc.tEventOldestMs < first) {
            first = acc.tEventOldestMs;
            firstState.update(first);
        }
        TimeWindow w = ctx.window();
        FeatureVector fv = new FeatureVector();
        fv.caseId = key;
        fv.windowStartMs = w.getStart();
        fv.windowEndMs = w.getEnd();
        fv.partial = w.getStart() < first;
        fv.watermarkAtFireMs = ctx.currentWatermark();
        fv.tWindowFiredMs = System.currentTimeMillis();
        fv.nRecords = acc.nRecords;
        fv.nWave = acc.nWave;
        fv.nNumeric = acc.nNumeric;
        fv.tEventOldestMs = acc.tEventOldestMs;
        fv.tEventNewestMs = acc.tEventNewestMs;
        fv.tAppendOldestMs = acc.tAppendOldestMs;
        fv.tAppendNewestMs = acc.tAppendNewestMs;
        fv.appendTimeAuthoritative = acc.appendTimeAuthoritative;
        fv.features = acc.labels;
        fv.subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        out.collect(fv);
    }
}
