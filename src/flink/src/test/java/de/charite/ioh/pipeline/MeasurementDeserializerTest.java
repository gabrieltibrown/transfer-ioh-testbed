package de.charite.ioh.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fixtures under src/test/resources were produced by the harness's own record
 * builders (records.py); see the generating snippet in the sprint 03-06 findings.
 */
class MeasurementDeserializerTest {
    private static JsonNode fixture(String name) throws Exception {
        try (InputStream in = MeasurementDeserializerTest.class.getResourceAsStream("/" + name)) {
            return new ObjectMapper().readTree(in);
        }
    }

    @Test
    void dwcTimestampParsesToEpochMillis() {
        assertEquals(1791366600123L, DwcTime.parseMs("2026-10-07 09:50:00.123 +00:00"));
        assertEquals(1791366600000L, DwcTime.parseMs("2026-10-07 09:50:00.000 +00:00"));
        // a non-UTC offset is honoured
        assertEquals(1791366600123L - 3600_000L, DwcTime.parseMs("2026-10-07 09:50:00.123 +01:00"));
    }

    @Test
    void waveRecordEventTimeComesFromDwcFieldsAndMatchesHarness() throws Exception {
        JsonNode n = fixture("wave.json");
        Measurement m = MeasurementDeserializer.parse(n, true);
        assertTrue(m.isWave());
        assertEquals("42", m.caseId);
        assertEquals("ART", m.label);
        assertEquals(3, m.seq);
        assertEquals(1250, m.nSamples);
        assertEquals(125.0, m.hz);
        assertEquals(1791366600123L, m.eventTimeFirstMs);
        // last sample = first + (n-1) * 8 ms, and that equals the harness's _event_ts_last to the ms
        assertEquals(1791366600123L + 1249 * 8, m.eventTimeMs);
        assertEquals(Math.round(n.get("_event_ts_last").asDouble() * 1000), m.eventTimeMs, 1.0);
        assertArrayEquals(new int[]{7}, m.invalidIdx);
        assertArrayEquals(new int[]{1248, 1249}, m.unavailableIdx);
        assertEquals(1250, m.samples.length);
    }

    @Test
    void scaleRangeSpecReproducesGainBiasDecode() throws Exception {
        Measurement m = MeasurementDeserializer.parse(fixture("wave.json"), true);
        double gain = 0.9874570312500001, bias = 31861.366;
        for (int raw : new int[]{-32768, -32000, 0, 1234, 32767}) {
            assertEquals(raw * gain + bias, m.physical(raw), 1e-6);
        }
    }

    @Test
    void numericRecordParses() throws Exception {
        JsonNode n = fixture("numeric.json");
        Measurement m = MeasurementDeserializer.parse(n, false);
        assertFalse(m.isWave());
        assertEquals("HR", m.label);
        assertEquals(9, m.seq);
        assertEquals(81.5, m.value);
        assertEquals(1791366601123L, m.eventTimeMs);
        assertEquals(m.eventTimeFirstMs, m.eventTimeMs);
        assertEquals(Math.round(n.get("_event_ts").asDouble() * 1000), m.eventTimeMs, 1.0);
        assertEquals(0, m.samples.length);
    }
}
