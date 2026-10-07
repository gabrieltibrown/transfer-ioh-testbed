package de.charite.ioh.pipeline;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Passes measurements through and, every {@code intervalMs} of processing time
 * per key, emits a {@link Progress} heartbeat to a side output. Sits before the
 * window so it observes a case's progress independently of prediction cadence.
 * Back-pressure from inference reaches this operator through the source, so a
 * saturated pipeline shows as the heartbeat's event time falling behind.
 */
public class ProgressFunction extends KeyedProcessFunction<String, Measurement, Measurement> {
    private static final long serialVersionUID = 1L;

    public static final OutputTag<Progress> PROGRESS = new OutputTag<Progress>("progress") {};

    private final long intervalMs;
    private transient ValueState<Progress> state;

    public ProgressFunction(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    @Override
    public void open(OpenContext openContext) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("progress", Progress.class));
    }

    @Override
    public void processElement(Measurement m, Context ctx, Collector<Measurement> out) throws Exception {
        Progress p = state.value();
        if (p == null) {
            p = new Progress();
            p.caseId = m.caseId;
            p.subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            long now = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(now - (now % intervalMs) + intervalMs);
        }
        p.recordsSeen++;
        if (m.isWave()) {
            p.wavesSeen++;
        } else {
            p.numericsSeen++;
        }
        p.tEventNewestSeenMs = Math.max(p.tEventNewestSeenMs, m.eventTimeMs);
        p.tAppendNewestSeenMs = Math.max(p.tAppendNewestSeenMs, m.tAppendMs);
        state.update(p);
        out.collect(m);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Measurement> out) throws Exception {
        Progress p = state.value();
        if (p == null) {
            return;
        }
        Progress snap = new Progress();
        snap.caseId = p.caseId;
        snap.tWallMs = System.currentTimeMillis();
        snap.tEventNewestSeenMs = p.tEventNewestSeenMs;
        snap.tAppendNewestSeenMs = p.tAppendNewestSeenMs;
        snap.watermarkMs = ctx.timerService().currentWatermark();
        snap.recordsSeen = p.recordsSeen;
        snap.wavesSeen = p.wavesSeen;
        snap.numericsSeen = p.numericsSeen;
        snap.subtask = p.subtask;
        ctx.output(PROGRESS, snap);
        ctx.timerService().registerProcessingTimeTimer(timestamp + intervalMs);
    }
}
