package com.eventpulse.importer;

import java.util.ArrayList;
import java.util.List;

/** RFC-4180-compatible parser for the private event exports. */
public final class CsvParser {
    private CsvParser() { }

    public static char detectDelimiter(String line) {
        int commas = 0, semicolons = 0;
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') i++;
                else quoted = !quoted;
            } else if (!quoted) {
                if (c == ',') commas++;
                if (c == ';') semicolons++;
            }
        }
        return semicolons > commas ? ';' : ',';
    }

    public static List<String> parse(String line, char delimiter) {
        List<String> result = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    field.append('"'); i++;
                } else quoted = !quoted;
            } else if (c == delimiter && !quoted) {
                result.add(field.toString()); field.setLength(0);
            } else field.append(c);
        }
        result.add(field.toString());
        return result;
    }
}
