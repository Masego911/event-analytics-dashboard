package com.eventpulse.service;

import com.eventpulse.importer.CsvParser;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class DataSettingsService {

    private static final String EXCLUSIONS_FILE =
            "excluded_emails.txt";

    private static final String CATEGORIES_FILE =
            "categories.txt";

    private final CsvParser csvParser;
    private final CategoryService categoryService;

    public DataSettingsService(
            CsvParser csvParser,
            CategoryService categoryService) {

        this.csvParser =
                csvParser;

        this.categoryService =
                categoryService;
    }

    public Set<String> loadExcludedEmails(
            Path dataDirectory) {

        Set<String> excluded =
                new LinkedHashSet<>();

        Path file =
                dataDirectory.resolve(
                        EXCLUSIONS_FILE
                );

        if (!Files.exists(file)) {
            return excluded;
        }

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             file,
                             StandardCharsets.UTF_8
                     )) {

            String line;

            while ((line = reader.readLine()) != null) {

                String email =
                        line
                                .trim()
                                .toLowerCase(
                                        Locale.ROOT
                                );

                if (!email.isEmpty()) {

                    excluded.add(email);
                }
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to read excluded emails from "
                            + file,
                    exception
            );
        }

        return excluded;
    }

    public Map<String, String> loadCategoryOverrides(
            Path dataDirectory) {

        Map<String, String> overrides =
                new LinkedHashMap<>();

        Path file =
                dataDirectory.resolve(
                        CATEGORIES_FILE
                );

        if (!Files.exists(file)) {
            return overrides;
        }

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             file,
                             StandardCharsets.UTF_8
                     )) {

            String line;

            while ((line = reader.readLine()) != null) {

                if (line.isBlank()) {
                    continue;
                }

                List<String> columns =
                        csvParser.parseLine(line);

                if (columns.size() < 2) {
                    continue;
                }

                String show =
                        columns.get(0).trim();

                String category =
                        categoryService.normalizeCategory(
                                columns.get(1).trim()
                        );

                if (!show.isEmpty()
                        && category != null) {

                    overrides.put(
                            show,
                            category
                    );
                }
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to read category overrides from "
                            + file,
                    exception
            );
        }

        return overrides;
    }

    public void saveExcludedEmails(
            Path dataDirectory,
            Set<String> emails) {

        Path file =
                dataDirectory.resolve(
                        EXCLUSIONS_FILE
                );

        try (BufferedWriter writer =
                     Files.newBufferedWriter(
                             file,
                             StandardCharsets.UTF_8
                     )) {

            for (String email : emails) {

                if (email == null
                        || email.isBlank()) {

                    continue;
                }

                writer.write(
                        email
                                .trim()
                                .toLowerCase(
                                        Locale.ROOT
                                )
                );

                writer.newLine();
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to save excluded emails",
                    exception
            );
        }
    }

    public void saveCategoryOverrides(
            Path dataDirectory,
            Map<String, String> overrides) {

        Path file =
                dataDirectory.resolve(
                        CATEGORIES_FILE
                );

        try (BufferedWriter writer =
                     Files.newBufferedWriter(
                             file,
                             StandardCharsets.UTF_8
                     )) {

            for (Map.Entry<String, String> entry :
                    overrides.entrySet()) {

                String category =
                        categoryService.normalizeCategory(
                                entry.getValue()
                        );

                if (entry.getKey() == null
                        || entry.getKey().isBlank()
                        || category == null) {

                    continue;
                }

                writer.write(
                        escapeCsv(entry.getKey())
                );

                writer.write(',');

                writer.write(
                        escapeCsv(category)
                );

                writer.newLine();
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to save category overrides",
                    exception
            );
        }
    }

    private String escapeCsv(String value) {

        if (value == null) {
            return "";
        }

        if (value.contains(",")
                || value.contains("\"")
                || value.contains("\n")
                || value.contains("\r")) {

            return "\""
                    + value.replace(
                            "\"",
                            "\"\""
                    )
                    + "\"";
        }

        return value;
    }
}
