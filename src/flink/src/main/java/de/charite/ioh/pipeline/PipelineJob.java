package de.charite.ioh.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * replay -> Kafka -> [this job] -> inference stub -> predictions topic -> interface.
 *
 * <pre>
 *   KafkaSource(dwc-waveform, dwc-numeric)        per-partition watermarks
 *     -> keyBy(caseId)
 *     -> ProgressFunction                          side output: progress topic
 *     -> SlidingEventTimeWindows(window, slide)    FeatureAggregate + FeatureWindowFunction
 *     -> AsyncDataStream.unorderedWait             InferenceAsyncFunction, bounded capacity
 *     -> KafkaSink(predictions)
 * </pre>
 */
public final class PipelineJob {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.fromArgs(args);
        LOG.info("ioh-pipeline config {}", cfg.describe());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (cfg.parallelism > 0) {
            env.setParallelism(cfg.parallelism);
        }
        if (cfg.checkpointMs > 0) {
            env.enableCheckpointing(cfg.checkpointMs);
        }

        KafkaSource<Measurement> source = KafkaSource.<Measurement>builder()
                .setBootstrapServers(cfg.bootstrap)
                .setTopics(cfg.waveTopic, cfg.numericTopic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new MeasurementDeserializer(cfg.waveTopic))
                .build();

        WatermarkStrategy<Measurement> wm = WatermarkStrategy
                .<Measurement>forBoundedOutOfOrderness(Duration.ofMillis(cfg.watermarkBoundMs))
                .withTimestampAssigner((m, ts) -> m.eventTimeMs)
                .withIdleness(Duration.ofMillis(cfg.idlenessMs));

        DataStream<Measurement> measurements = env.fromSource(source, wm, "dwc-source");

        SingleOutputStreamOperator<Measurement> tracked = measurements
                .keyBy(m -> m.caseId)
                .process(new ProgressFunction(cfg.progressIntervalMs))
                .name("progress");

        tracked.getSideOutput(ProgressFunction.PROGRESS)
                .sinkTo(sink(cfg, cfg.progressTopic, Json::progress, p -> p.caseId))
                .name("progress-sink");

        DataStream<FeatureVector> features = tracked
                .keyBy(m -> m.caseId)
                .window(SlidingEventTimeWindows.of(Duration.ofMillis(cfg.windowMs), Duration.ofMillis(cfg.slideMs)))
                .aggregate(new FeatureAggregate(), new FeatureWindowFunction())
                .name("features");

        DataStream<Prediction> predictions = AsyncDataStream.unorderedWait(
                features,
                new InferenceAsyncFunction(cfg.inferenceUrl, cfg.inferenceTimeoutMs),
                cfg.inferenceTimeoutMs, TimeUnit.MILLISECONDS,
                cfg.inferenceCapacity).name("inference");

        predictions
                .sinkTo(sink(cfg, cfg.predictionsTopic, Json::prediction, p -> p.window.caseId))
                .name("predictions-sink");

        env.execute("ioh-pipeline " + cfg.runId);
    }

    interface Encoder<T> extends java.io.Serializable {
        byte[] apply(T t);
    }

    interface KeyOf<T> extends java.io.Serializable {
        String apply(T t);
    }

    private static <T> KafkaSink<T> sink(JobConfig cfg, String topic, Encoder<T> enc, KeyOf<T> key) {
        Properties props = new Properties();
        props.setProperty("linger.ms", "0");
        return KafkaSink.<T>builder()
                .setBootstrapServers(cfg.bootstrap)
                .setKafkaProducerConfig(props)
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema(t -> key.apply(t).getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(enc::apply)
                        .build())
                .build();
    }
}
