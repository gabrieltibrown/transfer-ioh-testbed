package de.charite.ioh.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;

/**
 * JSON wire records (docs/stream-model.md 1.7) to {@link Measurement}. The Kafka
 * record's timestamp and timestamp type are captured here because they are not
 * in the payload: LogAppendTime is stamped by the broker.
 *
 * <p>Event time comes from the DWC fields so the job is unchanged when the
 * source is the real DWC feed: {@code c_time_stamp_wave_sample + (n-1) *
 * c_sample_period} for waves, {@code c_time_stamp} for numerics.
 */
public class MeasurementDeserializer implements KafkaRecordDeserializationSchema<Measurement> {
    private static final long serialVersionUID = 1L;
    private static final int[] EMPTY = new int[0];

    private final String waveTopic;
    private transient ObjectMapper mapper;

    public MeasurementDeserializer(String waveTopic) {
        this.waveTopic = waveTopic;
    }

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Measurement> out) throws IOException {
        Measurement m = parse(mapper().readTree(record.value()), record.topic().equals(waveTopic));
        m.tAppendMs = record.timestamp();
        m.appendTimeAuthoritative = record.timestampType() == TimestampType.LOG_APPEND_TIME;
        m.partition = record.partition();
        m.offset = record.offset();
        out.collect(m);
    }

    /** Payload-only parse, for tests and for the reference fixture. */
    public static Measurement parse(JsonNode n, boolean wave) {
        Measurement m = new Measurement();
        m.caseId = n.path("p_patnr").asText();
        m.label = n.path("c_label").asText();
        m.seq = n.path("c_sequence_number").asLong();
        m.harnessEventTs = n.path("_event_ts").asDouble(Double.NaN);
        m.harnessTProduce = n.path("_t_produce").asDouble(Double.NaN);
        if (wave) {
            m.kind = Measurement.WAVE;
            m.nSamples = n.path("c_n_samples").asInt();
            m.hz = n.path("c_hz").asDouble();
            double periodMs = n.path("c_sample_period").asDouble();
            m.eventTimeFirstMs = DwcTime.parseMs(n.path("c_time_stamp_wave_sample").asText());
            m.eventTimeMs = m.eventTimeFirstMs + Math.round((m.nSamples - 1) * periodMs);
            m.samples = intArray(n.path("c_value"));
            m.scaleLower = n.path("c_scale_lower").asInt();
            m.scaleUpper = n.path("c_scale_upper").asInt();
            m.calibrationLower = n.path("c_calibration_abs_lower").asDouble();
            m.calibrationUpper = n.path("c_calibration_abs_upper").asDouble();
            m.invalidIdx = intArray(n.path("c_invalid_samples"));
            m.unavailableIdx = intArray(n.path("c_unavailable_samples"));
        } else {
            m.kind = Measurement.NUMERIC;
            m.eventTimeFirstMs = DwcTime.parseMs(n.path("c_time_stamp").asText());
            m.eventTimeMs = m.eventTimeFirstMs;
            m.value = n.path("c_value").asDouble();
            m.samples = EMPTY;
            m.invalidIdx = EMPTY;
            m.unavailableIdx = EMPTY;
        }
        return m;
    }

    private static int[] intArray(JsonNode arr) {
        if (arr == null || !arr.isArray()) {
            return EMPTY;
        }
        int[] out = new int[arr.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = arr.get(i).asInt();
        }
        return out;
    }

    @Override
    public TypeInformation<Measurement> getProducedType() {
        return TypeInformation.of(Measurement.class);
    }
}
