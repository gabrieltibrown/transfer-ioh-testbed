package de.charite.ioh.pipeline;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Concrete Kafka record serializers for the output topics. Concrete classes
 * rather than the builder with lambdas: Flink 2.2's builder wraps the value
 * schema for lineage and cannot infer a lambda's type parameter.
 */
public final class JsonSerializers {
    private JsonSerializers() {}

    public static final class ProgressSerializer implements KafkaRecordSerializationSchema<Progress> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        public ProgressSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Progress p, KafkaSinkContext ctx, @Nullable Long timestamp) {
            return new ProducerRecord<>(topic, p.caseId.getBytes(StandardCharsets.UTF_8), Json.progress(p));
        }
    }

    public static final class PredictionSerializer implements KafkaRecordSerializationSchema<Prediction> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        public PredictionSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Prediction p, KafkaSinkContext ctx, @Nullable Long timestamp) {
            return new ProducerRecord<>(topic, p.window.caseId.getBytes(StandardCharsets.UTF_8), Json.prediction(p));
        }
    }
}
