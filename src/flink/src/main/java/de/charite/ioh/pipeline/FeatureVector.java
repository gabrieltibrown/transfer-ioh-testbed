package de.charite.ioh.pipeline;

import java.util.HashMap;

/** One fired window for one case, with everything the metrics need attached. */
public class FeatureVector {
    public String caseId;
    public long windowStartMs;
    public long windowEndMs;
    /** The window starts before the case's first event: it covers less than its full span. */
    public boolean partial;
    public long watermarkAtFireMs;
    /** TaskManager clock when the window fired. */
    public long tWindowFiredMs;
    public long nRecords;
    public long nWave;
    public long nNumeric;
    public long tEventOldestMs;
    public long tEventNewestMs;
    public long tAppendOldestMs;
    public long tAppendNewestMs;
    public boolean appendTimeAuthoritative;
    public HashMap<String, LabelStats> features = new HashMap<>();
    public int subtask;

    public FeatureVector() {}
}
