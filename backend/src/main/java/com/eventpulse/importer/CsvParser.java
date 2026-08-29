package com.eventpulse.importer;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CsvParser {

    public char detectDelimiter(String line) {

        if (line == null || line.isEmpty()) {
            return ',';
        }

        int commas = 0;
        int semicolons = 0;

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char character =
                    line.charAt(i);

            if (character == '"') {

                if (insideQuotes
                        && i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    i++;

                } else {

                    insideQuotes =
                            !insideQuotes;
                }

            } else if (!insideQuotes) {

                if (character == ',') {
                    commas++;
                }

                if (character == ';') {
                    semicolons++;
                }
            }
        }

        return semicolons > commas
                ? ';'
                : ',';
    }

    public List<String> parseLine(
            String line) {

        return parseLine(
                line,
                detectDelimiter(line)
        );
    }

    public List<String> parseLine(
            String line,
            char delimiter) {

        List<String> values =
                new ArrayList<>();

        StringBuilder current =
                new StringBuilder();

        boolean insideQuotes =
                false;

        for (int i = 0; i < line.length(); i++) {

            char character =
                    line.charAt(i);

            if (insideQuotes) {

                if (character == '"') {

                    if (i + 1 < line.length()
                            && line.charAt(i + 1) == '"') {

                        current.append('"');
                        i++;

                    } else {

                        insideQuotes = false;
                    }

                } else {

                    current.append(character);
                }

            } else {

                if (character == '"') {

                    insideQuotes = true;

                } else if (character == delimiter) {

                    values.add(
                            current.toString()
                    );

                    current.setLength(0);

                } else {

                    current.append(character);
                }
            }
        }

        values.add(
                current.toString()
        );

        return values;
    }

    public int findColumn(
            List<String> headers,
            String name) {

        for (int i = 0;
             i < headers.size();
             i++) {

            if (headers
                    .get(i)
                    .trim()
                    .equalsIgnoreCase(name)) {

                return i;
            }
        }

        return -1;
    }

    public int findAnyColumn(
            List<String> headers,
            String... names) {

        for (String name : names) {

            int index =
                    findColumn(
                            headers,
                            name
                    );

            if (index >= 0) {
                return index;
            }
        }

        return -1;
    }

    public String value(
            List<String> row,
            int index) {

        if (index < 0
                || index >= row.size()) {

            return "";
        }

        String value =
                row.get(index);

        return value == null
                ? ""
                : value.trim();
    }

    public double parsePrice(
            String rawValue) {

        if (rawValue == null) {
            return 0;
        }

        String value =
                rawValue
                        .trim()
                        .replaceAll(
                                "[^0-9,.-]",
                                ""
                        );

        if (value.isEmpty()
                || value.equals("-")
                || value.equals(".")
                || value.equals(",")) {

            return 0;
        }

        int comma =
                value.lastIndexOf(',');

        int dot =
                value.lastIndexOf('.');

        if (comma >= 0 && dot >= 0) {

            if (comma > dot) {

                value =
                        value
                                .replace(".", "")
                                .replace(',', '.');

            } else {

                value =
                        value.replace(",", "");
            }

        } else if (comma >= 0) {

            int decimals =
                    value.length()
                            - comma
                            - 1;

            if (decimals == 1
                    || decimals == 2) {

                value =
                        value.replace(',', '.');

            } else {

                value =
                        value.replace(",", "");
            }
        }

        try {

            return Double.parseDouble(value);

        } catch (NumberFormatException exception) {

            return 0;
        }
    }
}
