package de.charite.ioh.pipeline;

/**
 * One record off a DWC topic: a wave packet or a numeric value. Flink POJO
 * (public fields, no-arg constructor) so it is serialized without Kryo.
 *
 * <p>Two timestamps matter downstream: {@code eventTimeMs}, derived from the
 * DWC timestamp fields (the last sample of a wave packet, the sample itself for
 * a numeric), and {@code tAppendMs}, the broker's LogAppendTime, which is the
 * testbed's ingress reference for pipeline latency.
 */
public class Measurement {
    public static final int WAVE = 0;
    public static final int NUMERIC = 1;

    public int kind;
    public String caseId;
    public String label;
    public long seq;
    /** Event time of the first sample (wave) or the value (numeric), epoch ms. */
    public long eventTimeFirstMs;
    /** Event time of the last sample; equals eventTimeFirstMs for numerics. */
    public long eventTimeMs;
    /** Kafka record timestamp (LogAppendTime), epoch ms. */
    public long tAppendMs;
    /** True when the broker stamped the record with LogAppendTime, as configured. */
    public boolean appendTimeAuthoritative;
    public int partition;
    public long offset;

    // numeric
    public double value;

    // wave
    public int[] samples;
    public int nSamples;
    public double hz;
    public double calibrationLower;
    public double calibrationUpper;
    public int scaleLower;
    public int scaleUpper;
    public int[] invalidIdx;
    public int[] unavailableIdx;

    // testbed cross-check fields carried from the harness
    public double harnessEventTs;
    public double harnessTProduce;

    public Measurement() {}

    public boolean isWave() {
        return kind == WAVE;
    }

    /** Consumer-side ScaleRangeSpec16 linear map, the DWC contract. */
    public double physical(int raw) {
        return calibrationLower + (raw - (double) scaleLower) * (calibrationUpper - calibrationLower)
                / ((double) scaleUpper - scaleLower);
    }
}
