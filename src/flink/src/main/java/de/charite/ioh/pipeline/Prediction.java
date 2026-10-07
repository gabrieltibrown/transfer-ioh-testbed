package de.charite.ioh.pipeline;

import com.fasterxml.jackson.databind.JsonNode;

/** A feature vector after inference, as delivered to the interface. */
public class Prediction {
    public FeatureVector window;
    /** ok, timeout, error, rejected. */
    public String status;
    public int httpStatus;
    /** TaskManager clock, epoch ms. */
    public long tInferenceSentMs;
    public long tInferenceReturnedMs;
    public double risk = Double.NaN;
    /** The stub's own response body, verbatim, for the queueing decomposition. */
    public JsonNode stub;
    public String error;
    public int subtask;

    public Prediction() {}
}
