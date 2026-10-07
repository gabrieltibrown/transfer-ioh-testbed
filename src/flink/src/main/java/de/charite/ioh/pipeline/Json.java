package de.charite.ioh.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** JSON for the output topics. Field names are the ones the interface consumer and analysis read. */
public final class Json {
    private static final ObjectMapper M = new ObjectMapper();

    private Json() {}

    public static byte[] progress(Progress p) {
        try {
            ObjectNode n = M.createObjectNode();
            n.put("type", "progress");
            n.put("caseId", p.caseId);
            n.put("tWallMs", p.tWallMs);
            n.put("tEventNewestSeenMs", p.tEventNewestSeenMs);
            n.put("tAppendNewestSeenMs", p.tAppendNewestSeenMs);
            n.put("watermarkMs", p.watermarkMs);
            n.put("recordsSeen", p.recordsSeen);
            n.put("wavesSeen", p.wavesSeen);
            n.put("numericsSeen", p.numericsSeen);
            n.put("subtask", p.subtask);
            return M.writeValueAsBytes(n);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] prediction(Prediction p) {
        try {
            FeatureVector w = p.window;
            ObjectNode n = M.createObjectNode();
            n.put("type", "prediction");
            n.put("caseId", w.caseId);
            n.put("windowStartMs", w.windowStartMs);
            n.put("windowEndMs", w.windowEndMs);
            n.put("partial", w.partial);
            n.put("watermarkAtFireMs", w.watermarkAtFireMs);
            n.put("tWindowFiredMs", w.tWindowFiredMs);
            n.put("nRecords", w.nRecords);
            n.put("nWave", w.nWave);
            n.put("nNumeric", w.nNumeric);
            n.put("tEventOldestMs", w.tEventOldestMs);
            n.put("tEventNewestMs", w.tEventNewestMs);
            n.put("tAppendOldestMs", w.tAppendOldestMs);
            n.put("tAppendNewestMs", w.tAppendNewestMs);
            n.put("appendTimeAuthoritative", w.appendTimeAuthoritative);
            n.put("windowSubtask", w.subtask);
            ObjectNode feats = n.putObject("features");
            w.features.forEach((label, s) -> {
                ObjectNode f = feats.putObject(label);
                f.put("kind", s.kind == Measurement.WAVE ? "wave" : "numeric");
                f.put("mean", s.mean());
                f.put("min", s.min);
                f.put("max", s.max);
                f.put("count", s.count);
                f.put("records", s.records);
                if (s.kind == Measurement.WAVE) {
                    f.put("invalid", s.invalid);
                } else {
                    f.put("last", s.last);
                }
            });
            ObjectNode inf = n.putObject("inference");
            inf.put("status", p.status);
            inf.put("httpStatus", p.httpStatus);
            inf.put("tSentMs", p.tInferenceSentMs);
            inf.put("tReturnedMs", p.tInferenceReturnedMs);
            inf.put("risk", p.risk);
            inf.put("subtask", p.subtask);
            if (p.error != null) {
                inf.put("error", p.error);
            }
            if (p.stub != null) {
                inf.set("stub", p.stub);
            }
            return M.writeValueAsBytes(n);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
