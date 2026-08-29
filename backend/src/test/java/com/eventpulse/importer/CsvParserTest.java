package com.eventpulse.importer;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvParserTest {
    private final CsvParser parser = new CsvParser();

    @Test
    void parsesQuotedCommaAndDetectsSemicolon() {
        assertEquals(',', parser.detectDelimiter("Name,Email,Phone"));
        assertEquals(List.of("A, B", "person@example.com", "082"),
                parser.parseLine("\"A, B\",person@example.com,082", ','));
        assertEquals(';', parser.detectDelimiter("Name;Email;Phone"));
        assertEquals(List.of("A;B", "person@example.com"),
                parser.parseLine("\"A;B\";person@example.com", ';'));
    }
}
