package de.charite.ioh.pipeline;

/**
 * Per-case progress heartbeat, emitted on a processing-time timer. Per-case
 * progress lag is {@code tWallMs - tEventNewestSeenMs} once clock domains are
 * reconciled (the TaskManager clock stamps tWallMs; event time is host-stamped).
 */
public class Progress {
    public String caseId;
    public long tWallMs;
    public long tEventNewestSeenMs;
    public long tAppendNewestSeenMs;
    public long watermarkMs;
    public long recordsSeen;
    public long wavesSeen;
    public long numericsSeen;
    public int subtask;

    public Progress() {}
}
