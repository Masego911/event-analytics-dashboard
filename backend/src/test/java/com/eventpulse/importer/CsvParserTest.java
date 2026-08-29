package com.eventpulse.importer;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvParserTest {
    @Test
    void parsesQuotedCommaAndDetectsSemicolon() {
        assertEquals(',', CsvParser.detectDelimiter("Name,Email,Phone"));
        assertEquals(List.of("A, B", "person@example.com", "082"),
                CsvParser.parse("\"A, B\",person@example.com,082", ','));
        assertEquals(';', CsvParser.detectDelimiter("Name;Email;Phone"));
        assertEquals(List.of("A;B", "person@example.com"),
                CsvParser.parse("\"A;B\";person@example.com", ';'));
    }
}
