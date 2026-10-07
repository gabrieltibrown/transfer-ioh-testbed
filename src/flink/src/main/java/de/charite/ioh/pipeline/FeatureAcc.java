package de.charite.ioh.pipeline;

import java.util.HashMap;

/** Incremental window accumulator: per-label statistics plus the timing bounds the metrics need. */
public class FeatureAcc {
    public HashMap<String, LabelStats> labels = new HashMap<>();
    public long nRecords;
    public long nWave;
    public long nNumeric;
    public long tEventOldestMs = Long.MAX_VALUE;
    public long tEventNewestMs = Long.MIN_VALUE;
    public long tAppendOldestMs = Long.MAX_VALUE;
    public long tAppendNewestMs = Long.MIN_VALUE;
    public boolean appendTimeAuthoritative = true;

    public FeatureAcc() {}

    public void add(Measurement m) {
        LabelStats s = labels.computeIfAbsent(m.label, k -> {
            LabelStats n = new LabelStats();
            n.kind = m.kind;
            return n;
        });
        if (m.isWave()) {
            s.addWave(m);
            nWave++;
        } else {
            s.addNumeric(m.value, m.eventTimeMs);
            nNumeric++;
        }
        nRecords++;
        tEventOldestMs = Math.min(tEventOldestMs, m.eventTimeFirstMs);
        tEventNewestMs = Math.max(tEventNewestMs, m.eventTimeMs);
        tAppendOldestMs = Math.min(tAppendOldestMs, m.tAppendMs);
        tAppendNewestMs = Math.max(tAppendNewestMs, m.tAppendMs);
        appendTimeAuthoritative &= m.appendTimeAuthoritative;
    }

    public FeatureAcc merge(FeatureAcc o) {
        FeatureAcc r = new FeatureAcc();
        r.labels = new HashMap<>(labels);
        o.labels.forEach((k, v) -> r.labels.merge(k, v, LabelStats::merge));
        r.nRecords = nRecords + o.nRecords;
        r.nWave = nWave + o.nWave;
        r.nNumeric = nNumeric + o.nNumeric;
        r.tEventOldestMs = Math.min(tEventOldestMs, o.tEventOldestMs);
        r.tEventNewestMs = Math.max(tEventNewestMs, o.tEventNewestMs);
        r.tAppendOldestMs = Math.min(tAppendOldestMs, o.tAppendOldestMs);
        r.tAppendNewestMs = Math.max(tAppendNewestMs, o.tAppendNewestMs);
        r.appendTimeAuthoritative = appendTimeAuthoritative && o.appendTimeAuthoritative;
        return r;
    }
}
