package de.charite.ioh.pipeline;

/** Running statistics for one label inside a window. */
public class LabelStats {
    public int kind;
    public double sum;
    public double min = Double.POSITIVE_INFINITY;
    public double max = Double.NEGATIVE_INFINITY;
    public long count;      // valid samples (wave) or values (numeric)
    public long invalid;    // wave: invalid + unavailable samples excluded
    public long records;
    public double last;     // numeric: value with the newest event time
    public long lastEventMs = Long.MIN_VALUE;

    public LabelStats() {}

    public void addNumeric(double v, long eventMs) {
        sum += v;
        min = Math.min(min, v);
        max = Math.max(max, v);
        count++;
        records++;
        if (eventMs >= lastEventMs) {
            lastEventMs = eventMs;
            last = v;
        }
    }

    public void addWave(Measurement m) {
        records++;
        boolean[] skip = new boolean[m.samples.length];
        for (int i : m.invalidIdx) {
            if (i >= 0 && i < skip.length) skip[i] = true;
        }
        for (int i : m.unavailableIdx) {
            if (i >= 0 && i < skip.length) skip[i] = true;
        }
        for (int i = 0; i < m.samples.length; i++) {
            if (skip[i]) {
                invalid++;
                continue;
            }
            double v = m.physical(m.samples[i]);
            sum += v;
            if (v < min) min = v;
            if (v > max) max = v;
            count++;
        }
        if (m.eventTimeMs >= lastEventMs) {
            lastEventMs = m.eventTimeMs;
        }
    }

    public LabelStats merge(LabelStats o) {
        LabelStats r = new LabelStats();
        r.kind = kind;
        r.sum = sum + o.sum;
        r.min = Math.min(min, o.min);
        r.max = Math.max(max, o.max);
        r.count = count + o.count;
        r.invalid = invalid + o.invalid;
        r.records = records + o.records;
        if (o.lastEventMs >= lastEventMs) {
            r.lastEventMs = o.lastEventMs;
            r.last = o.last;
        } else {
            r.lastEventMs = lastEventMs;
            r.last = last;
        }
        return r;
    }

    public double mean() {
        return count == 0 ? Double.NaN : sum / count;
    }
}
