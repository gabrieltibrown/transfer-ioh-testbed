package de.charite.ioh.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;

/**
 * Calls the inference service without blocking the operator. The number of
 * in-flight requests per subtask is bounded by the capacity given to
 * {@code AsyncDataStream.unorderedWait}; when the service is slower than demand
 * the operator fills to capacity and back-pressures the source. Timeouts and
 * errors become predictions with a non-ok status so the run completes and the
 * failure is counted at the interface.
 */
public class InferenceAsyncFunction extends RichAsyncFunction<FeatureVector, Prediction> {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final long timeoutMs;
    private transient HttpClient client;
    private transient ObjectMapper mapper;

    public InferenceAsyncFunction(String url, long timeoutMs) {
        this.url = url;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void open(OpenContext openContext) {
        client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
        mapper = new ObjectMapper();
    }

    @Override
    public void asyncInvoke(FeatureVector fv, ResultFuture<Prediction> result) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        body.put("caseId", fv.caseId);
        body.put("windowEndMs", fv.windowEndMs);
        ObjectNode feats = body.putObject("features");
        fv.features.forEach((label, s) -> {
            ObjectNode f = feats.putObject(label);
            f.put("mean", s.mean());
            f.put("min", s.min);
            f.put("max", s.max);
            f.put("last", s.last);
            f.put("count", s.count);
        });
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                .build();
        final long tSent = System.currentTimeMillis();
        final int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        client.sendAsync(req, HttpResponse.BodyHandlers.ofString()).whenComplete((resp, err) -> {
            Prediction p = new Prediction();
            p.window = fv;
            p.tInferenceSentMs = tSent;
            p.tInferenceReturnedMs = System.currentTimeMillis();
            p.subtask = subtask;
            if (err != null) {
                p.status = err.getClass().getSimpleName().contains("Timeout") ? "timeout" : "error";
                p.error = err.toString();
            } else {
                p.httpStatus = resp.statusCode();
                try {
                    JsonNode n = mapper.readTree(resp.body());
                    p.stub = n;
                    if (resp.statusCode() == 200) {
                        p.status = "ok";
                        p.risk = n.path("risk").asDouble(Double.NaN);
                    } else if (resp.statusCode() == 503) {
                        p.status = "rejected";
                    } else {
                        p.status = "error";
                        p.error = "http " + resp.statusCode();
                    }
                } catch (Exception e) {
                    p.status = "error";
                    p.error = e.toString();
                }
            }
            result.complete(Collections.singleton(p));
        });
    }

    @Override
    public void timeout(FeatureVector fv, ResultFuture<Prediction> result) {
        Prediction p = new Prediction();
        p.window = fv;
        p.status = "timeout";
        p.error = "async operator timeout after " + timeoutMs + " ms";
        p.tInferenceReturnedMs = System.currentTimeMillis();
        result.complete(Collections.singleton(p));
    }
}
