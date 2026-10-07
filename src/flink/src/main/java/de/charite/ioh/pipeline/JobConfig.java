package de.charite.ioh.pipeline;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Job parameters, all from program arguments ({@code --key value}) so that every
 * run's configuration is in the submission request that ioh-run records.
 */
public final class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public String bootstrap = "kafka:29092";
    public String waveTopic = "dwc-waveform";
    public String numericTopic = "dwc-numeric";
    public String predictionsTopic = "predictions";
    public String progressTopic = "progress";
    public String groupId = "ioh-flink";
    public long windowMs = 60_000;
    public long slideMs = 20_000;
    public long watermarkBoundMs = 500;
    public long idlenessMs = 5_000;
    public long progressIntervalMs = 1_000;
    public String inferenceUrl = "http://host.docker.internal:8000/predict";
    public int inferenceCapacity = 8;
    public long inferenceTimeoutMs = 10_000;
    public long checkpointMs = 10_000;
    public int parallelism = -1; // -1: cluster default
    public String runId = "";

    public static JobConfig fromArgs(String[] args) {
        JobConfig c = new JobConfig();
        Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                String val = (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : "true";
                kv.put(key, val);
            }
        }
        c.bootstrap = kv.getOrDefault("bootstrap", c.bootstrap);
        c.waveTopic = kv.getOrDefault("wave-topic", c.waveTopic);
        c.numericTopic = kv.getOrDefault("numeric-topic", c.numericTopic);
        c.predictionsTopic = kv.getOrDefault("predictions-topic", c.predictionsTopic);
        c.progressTopic = kv.getOrDefault("progress-topic", c.progressTopic);
        c.groupId = kv.getOrDefault("group-id", c.groupId);
        c.windowMs = longOf(kv, "window-ms", c.windowMs);
        c.slideMs = longOf(kv, "slide-ms", c.slideMs);
        c.watermarkBoundMs = longOf(kv, "watermark-bound-ms", c.watermarkBoundMs);
        c.idlenessMs = longOf(kv, "idleness-ms", c.idlenessMs);
        c.progressIntervalMs = longOf(kv, "progress-interval-ms", c.progressIntervalMs);
        c.inferenceUrl = kv.getOrDefault("inference-url", c.inferenceUrl);
        c.inferenceCapacity = (int) longOf(kv, "inference-capacity", c.inferenceCapacity);
        c.inferenceTimeoutMs = longOf(kv, "inference-timeout-ms", c.inferenceTimeoutMs);
        c.checkpointMs = longOf(kv, "checkpoint-ms", c.checkpointMs);
        c.parallelism = (int) longOf(kv, "parallelism", c.parallelism);
        c.runId = kv.getOrDefault("run-id", c.runId);
        if (c.windowMs <= 0 || c.slideMs <= 0 || c.windowMs % c.slideMs != 0) {
            throw new IllegalArgumentException("window-ms must be a positive multiple of slide-ms");
        }
        if (c.inferenceCapacity < 1) {
            throw new IllegalArgumentException("inference-capacity must be >= 1");
        }
        return c;
    }

    private static long longOf(Map<String, String> kv, String key, long dflt) {
        String v = kv.get(key);
        return v == null ? dflt : Long.parseLong(v);
    }

    public Map<String, Object> describe() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("bootstrap", bootstrap);
        m.put("wave_topic", waveTopic);
        m.put("numeric_topic", numericTopic);
        m.put("predictions_topic", predictionsTopic);
        m.put("progress_topic", progressTopic);
        m.put("group_id", groupId);
        m.put("window_ms", windowMs);
        m.put("slide_ms", slideMs);
        m.put("watermark_bound_ms", watermarkBoundMs);
        m.put("idleness_ms", idlenessMs);
        m.put("progress_interval_ms", progressIntervalMs);
        m.put("inference_url", inferenceUrl);
        m.put("inference_capacity_per_subtask", inferenceCapacity);
        m.put("inference_timeout_ms", inferenceTimeoutMs);
        m.put("checkpoint_ms", checkpointMs);
        m.put("parallelism", parallelism);
        m.put("run_id", runId);
        return m;
    }

    @Override
    public String toString() {
        return describe().toString();
    }
}
