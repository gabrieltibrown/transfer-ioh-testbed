package de.charite.ioh.pipeline;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/** DWC string timestamps: {@code 2024-09-26 23:41:53.480 +00:00}, millisecond precision. */
public final class DwcTime {
    private static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, true)
            .appendLiteral(' ')
            .appendOffset("+HH:MM", "+00:00")
            .toFormatter();

    private DwcTime() {}

    public static long parseMs(String s) {
        return OffsetDateTime.parse(s, FMT).toInstant().toEpochMilli();
    }
}
