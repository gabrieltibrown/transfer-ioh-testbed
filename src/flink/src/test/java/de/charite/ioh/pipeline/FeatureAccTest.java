package de.charite.ioh.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeatureAccTest {
    private static Measurement numeric(String label, double v, long t, long tAppend) {
        Measurement m = new Measurement();
        m.kind = Measurement.NUMERIC;
        m.caseId = "1";
        m.label = label;
        m.value = v;
        m.eventTimeFirstMs = m.eventTimeMs = t;
        m.tAppendMs = tAppend;
        m.appendTimeAuthoritative = true;
        m.samples = new int[0];
        m.invalidIdx = new int[0];
        m.unavailableIdx = new int[0];
        return m;
    }

    private static Measurement wave(String label, int[] samples, int[] invalid, int[] unavailable, long tFirst, long tLast) {
        Measurement m = new Measurement();
        m.kind = Measurement.WAVE;
        m.caseId = "1";
        m.label = label;
        m.samples = samples;
        m.nSamples = samples.length;
        m.invalidIdx = invalid;
        m.unavailableIdx = unavailable;
        m.scaleLower = -32768;
        m.scaleUpper = 32767;
        // identity map: physical == raw
        m.calibrationLower = -32768;
        m.calibrationUpper = 32767;
        m.eventTimeFirstMs = tFirst;
        m.eventTimeMs = tLast;
        m.tAppendMs = tLast + 5;
        m.appendTimeAuthoritative = true;
        return m;
    }

    @Test
    void numericStatsAndLastByEventTime() {
        FeatureAcc acc = new FeatureAcc();
        acc.add(numeric("HR", 80, 1000, 1010));
        acc.add(numeric("HR", 90, 3000, 3010));
        acc.add(numeric("HR", 70, 2000, 2010)); // out of order: not the last
        LabelStats s = acc.labels.get("HR");
        assertEquals(80.0, s.mean(), 1e-12);
        assertEquals(70.0, s.min);
        assertEquals(90.0, s.max);
        assertEquals(3, s.count);
        assertEquals(90.0, s.last);
        assertEquals(3, acc.nNumeric);
        assertEquals(1000, acc.tEventOldestMs);
        assertEquals(3000, acc.tEventNewestMs);
        assertEquals(1010, acc.tAppendOldestMs);
        assertEquals(3010, acc.tAppendNewestMs);
    }

    @Test
    void waveStatsSkipInvalidAndUnavailableSamples() {
        FeatureAcc acc = new FeatureAcc();
        acc.add(wave("ART", new int[]{10, 20, 30, 40, 50}, new int[]{1}, new int[]{4}, 1000, 1032));
        LabelStats s = acc.labels.get("ART");
        assertEquals(3, s.count);
        assertEquals(2, s.invalid);
        assertEquals((10 + 30 + 40) / 3.0, s.mean(), 1e-12);
        assertEquals(10.0, s.min);
        assertEquals(40.0, s.max);
        assertEquals(1, acc.nWave);
        assertEquals(1000, acc.tEventOldestMs);
        assertEquals(1032, acc.tEventNewestMs);
    }

    @Test
    void mergeCombinesLabelsAndBounds() {
        FeatureAcc a = new FeatureAcc();
        a.add(numeric("HR", 80, 1000, 1010));
        a.add(wave("ART", new int[]{1, 2}, new int[0], new int[0], 500, 508));
        FeatureAcc b = new FeatureAcc();
        b.add(numeric("HR", 100, 2000, 2010));
        b.add(numeric("SPO2", 98, 2000, 2010));
        Measurement notAuthoritative = numeric("HR", 60, 1500, 1510);
        notAuthoritative.appendTimeAuthoritative = false;
        b.add(notAuthoritative);
        FeatureAcc r = a.merge(b);
        assertEquals(3, r.labels.size());
        assertEquals(3, r.labels.get("HR").count);
        assertEquals(80.0, r.labels.get("HR").mean(), 1e-12);
        assertEquals(100.0, r.labels.get("HR").last);
        assertEquals(5, r.nRecords);
        assertEquals(500, r.tEventOldestMs);
        assertEquals(2000, r.tEventNewestMs);
        assertFalse(r.appendTimeAuthoritative);
        assertTrue(a.appendTimeAuthoritative);
    }

    @Test
    void jobConfigParsesAndValidates() {
        JobConfig c = JobConfig.fromArgs(new String[]{"--window-ms", "60000", "--slide-ms", "20000",
                "--inference-capacity", "16", "--run-id", "r1", "--bootstrap", "kafka:29092"});
        assertEquals(60000, c.windowMs);
        assertEquals(16, c.inferenceCapacity);
        assertEquals("r1", c.runId);
        assertEquals("kafka:29092", c.describe().get("bootstrap"));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromArgs(new String[]{"--window-ms", "50000", "--slide-ms", "20000"}));
    }
}
