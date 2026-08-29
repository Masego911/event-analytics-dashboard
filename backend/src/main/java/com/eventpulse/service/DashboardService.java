package com.eventpulse.service;

import com.eventpulse.dto.dashboard.CategorySummary;
import com.eventpulse.dto.dashboard.DashboardResponse;
import com.eventpulse.dto.dashboard.DashboardSummary;
import com.eventpulse.dto.dashboard.ShowSummary;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds the data required by the EventPulse audience dashboard.
 *
 * This service is the first extraction of real business behaviour
 * from the pre-migration server.
 *
 * It deliberately preserves the current React API contract while
 * moving processing out of the HTTP layer.
 */
@Service
public class DashboardService {

    private static final List<String> CATEGORIES = List.of(
            "Afrosoul",
            "Jazz",
            "Comedy",
            "Folk",
            "HipHop",
            "Reggae"
    );

    private final Path dataDirectory;
    private final CategoryResolver categoryResolver;

    /**
     * The data directory is configurable so private CSV exports remain
     * outside the Git repository.
     *
     * Current local default:
     *
     * database01/
     *   private CSV files
     *   event-analytics-dashboard/
     *     backend/
     *
     * Therefore ../../ resolves back to database01 when the backend is
     * started from its own directory.
     */
    public DashboardService(
            @Value("${eventpulse.data.directory:../..}") String dataDirectory
    ) {
        this.dataDirectory = Path.of(dataDirectory)
                .toAbsolutePath()
                .normalize();
        this.categoryResolver = new CategoryResolver();
    }


    /**
     * Creates the complete response expected by Dashboard.jsx.
     */
    public DashboardResponse getDashboard() {

        Map<String, String> categoryOverrides =
                loadCategoryOverrides();

        Set<String> excludedEmails =
                loadExcludedEmails();

        List<Path> csvFiles =
                findEventCsvFiles();

        List<ShowSummary> shows =
                new ArrayList<>();

        Map<String, Set<String>> categoryEmails =
                createCategorySetMap();

        Map<String, Set<String>> categoryPhones =
                createCategorySetMap();

        /*
         * One contact may occur in several CSV files.
         *
         * The contact key allows us to determine:
         * - unique audience size;
         * - which shows each person attended;
         * - returning audience count.
         */
        Map<String, ContactAccumulator> contacts =
                new LinkedHashMap<>();

        int totalTickets = 0;

        for (int showIndex = 0;
             showIndex < csvFiles.size();
             showIndex++) {

            Path csvFile = csvFiles.get(showIndex);

            String showName =
                    removeCsvExtension(
                            csvFile.getFileName().toString()
                    );

            String category =
                    categoryForShow(
                            showName,
                            categoryOverrides
                    );

            ShowProcessingResult showResult =
                    processShow(
                            csvFile,
                            showName,
                            category,
                            excludedEmails,
                            contacts
                    );

            totalTickets +=
                    showResult.ticketCount();

            categoryEmails
                    .get(category)
                    .addAll(showResult.emails());

            categoryPhones
                    .get(category)
                    .addAll(showResult.phones());

            shows.add(
                    new ShowSummary(
                            showIndex,
                            showName,
                            category,
                            showResult.emails().size(),
                            showResult.phones().size(),
                            showResult.ticketCount()
                    )
            );
        }

        int returningAudience =
                (int) contacts
                        .values()
                        .stream()
                        .filter(contact ->
                                contact.shows().size() > 1
                        )
                        .count();

        DashboardSummary summary =
                new DashboardSummary(
                        shows.size(),
                        contacts.size(),
                        totalTickets,
                        returningAudience
                );

        List<CategorySummary> categories =
                CATEGORIES
                        .stream()
                        .map(category ->
                                new CategorySummary(
                                        category,
                                        categoryEmails
                                                .get(category)
                                                .size(),
                                        categoryPhones
                                                .get(category)
                                                .size()
                                )
                        )
                        .toList();

        return new DashboardResponse(
                summary,
                shows,
                categories
        );
    }


    /**
     * Processes one Webtickets-style CSV export.
     */
    private ShowProcessingResult processShow(
            Path csvFile,
            String showName,
            String category,
            Set<String> excludedEmails,
            Map<String, ContactAccumulator> contacts
    ) {

        Set<String> showEmails =
                new LinkedHashSet<>();

        Set<String> showPhones =
                new LinkedHashSet<>();

        int ticketCount = 0;

        try (
                BufferedReader reader =
                        Files.newBufferedReader(
                                csvFile,
                                StandardCharsets.UTF_8
                        )
        ) {

            String headerLine =
                    reader.readLine();

            if (headerLine == null) {

                return new ShowProcessingResult(
                        showEmails,
                        showPhones,
                        0
                );
            }

            char delimiter =
                    detectDelimiter(headerLine);

            List<String> headers =
                    parseCsvLine(
                            headerLine,
                            delimiter
                    );

            int emailIndex =
                    findIndex(
                            headers,
                            "Email"
                    );

            int purchaserEmailIndex =
                    findIndex(
                            headers,
                            "Purchaser Email"
                    );

            int phoneIndex =
                    findIndexAny(
                            headers,
                            "Cellphone",
                            "Phone"
                    );

            int ticketIndex =
                    findIndexAny(
                            headers,
                            "Ticket Number",
                            "Barcode",
                            "Ticket Barcode"
                    );

            String line;

            int rowNumber = 0;

            while (
                    (line = reader.readLine())
                            != null
            ) {

                rowNumber++;

                if (line.isBlank()) {
                    continue;
                }

                List<String> row =
                        parseCsvLine(
                                line,
                                delimiter
                        );

                String email =
                        valueAt(
                                row,
                                emailIndex
                        )
                                .trim()
                                .toLowerCase(
                                        Locale.ROOT
                                );

                String purchaserEmail =
                        valueAt(
                                row,
                                purchaserEmailIndex
                        )
                                .trim()
                                .toLowerCase(
                                        Locale.ROOT
                                );

                /*
                 * The legacy implementation ignores a row when either
                 * email address is explicitly excluded.
                 */
                if (
                        excludedEmails.contains(email)
                                ||
                        excludedEmails.contains(
                                purchaserEmail
                        )
                ) {
                    continue;
                }

                String finalEmail =
                        !email.isBlank()
                                ? email
                                : purchaserEmail;

                String phone =
                        normalisePhone(
                                valueAt(
                                        row,
                                        phoneIndex
                                )
                        );

                String ticket =
                        valueAt(
                                row,
                                ticketIndex
                        ).trim();

                String contactKey =
                        buildContactKey(
                                finalEmail,
                                phone,
                                ticket,
                                showName,
                                rowNumber
                        );

                ContactAccumulator contact =
                        contacts.computeIfAbsent(
                                contactKey,
                                ignored ->
                                        new ContactAccumulator()
                        );

                if (!finalEmail.isBlank()) {

                    contact.setEmailIfMissing(
                            finalEmail
                    );

                    showEmails.add(
                            finalEmail
                    );
                }

                if (!phone.isBlank()) {

                    contact.setPhoneIfMissing(
                            phone
                    );

                    showPhones.add(
                            phone
                    );
                }

                contact.shows()
                        .add(showName);

                ticketCount++;
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to process CSV file: "
                            + csvFile.getFileName(),
                    exception
            );
        }

        return new ShowProcessingResult(
                showEmails,
                showPhones,
                ticketCount
        );
    }


    /**
     * Finds private event CSV exports without tracking them in Git.
     */
    private List<Path> findEventCsvFiles() {

        if (!Files.isDirectory(dataDirectory)) {

            return List.of();
        }

        try (var files =
                     Files.list(dataDirectory)) {

            return files
                    .filter(Files::isRegularFile)
                    .filter(path ->
                            path.getFileName()
                                    .toString()
                                    .toLowerCase(
                                            Locale.ROOT
                                    )
                                    .endsWith(".csv")
                    )
                    .filter(path -> {

                        String name =
                                path.getFileName()
                                        .toString();

                        return !name.equalsIgnoreCase(
                                "contacts_with_shows.csv"
                        )
                                &&
                                !name.equalsIgnoreCase(
                                        "all_contacts.csv"
                                );
                    })
                    .sorted(
                            Comparator.comparing(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .toLowerCase(
                                                            Locale.ROOT
                                                    )
                            )
                    )
                    .toList();

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to scan EventPulse data directory: "
                            + dataDirectory,
                    exception
            );
        }
    }


    /**
     * Reads categories.txt using:
     *
     * Show Name,Category
     */
    private Map<String, String> loadCategoryOverrides() {

        Path file =
                dataDirectory.resolve(
                        "categories.txt"
                );

        Map<String, String> overrides =
                new HashMap<>();

        if (!Files.isRegularFile(file)) {
            return overrides;
        }

        try (
                BufferedReader reader =
                        Files.newBufferedReader(
                                file,
                                StandardCharsets.UTF_8
                        )
        ) {

            String line;

            while (
                    (line = reader.readLine())
                            != null
            ) {

                if (line.isBlank()) {
                    continue;
                }

                List<String> values =
                        parseCsvLine(
                                line,
                                ','
                        );

                if (values.size() < 2) {
                    continue;
                }

                String show =
                        values.get(0)
                                .trim();

                String category =
                        normaliseCategory(
                                values.get(1)
                        );

                if (
                        !show.isBlank()
                                &&
                        category != null
                ) {

                    overrides.put(
                            show.toLowerCase(
                                    Locale.ROOT
                            ),
                            category
                    );
                }
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to read categories.txt",
                    exception
            );
        }

        return overrides;
    }


    /**
     * Reads excluded_emails.txt while keeping that file outside Git.
     */
    private Set<String> loadExcludedEmails() {

        Path file =
                dataDirectory.resolve(
                        "excluded_emails.txt"
                );

        Set<String> exclusions =
                new HashSet<>();

        if (!Files.isRegularFile(file)) {
            return exclusions;
        }

        try {

            for (
                    String line :
                    Files.readAllLines(
                            file,
                            StandardCharsets.UTF_8
                    )
            ) {

                String value =
                        line.trim()
                                .toLowerCase(
                                        Locale.ROOT
                                );

                if (!value.isBlank()) {
                    exclusions.add(value);
                }
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to read excluded_emails.txt",
                    exception
            );
        }

        return exclusions;
    }


    /**
     * Creates one email and phone set for every supported category.
     */
    private Map<String, Set<String>> createCategorySetMap() {

        Map<String, Set<String>> values =
                new LinkedHashMap<>();

        for (String category : CATEGORIES) {

            values.put(
                    category,
                    new LinkedHashSet<>()
            );
        }

        return values;
    }


    /**
     * Applies an explicit override first, then falls back to
     * the categorisation rules from the legacy application.
     */
    private String categoryForShow(
            String showName,
            Map<String, String> overrides
    ) {
        return categoryResolver.resolve(showName, overrides);
    }


    /**
     * Existing category rules retained for dashboard compatibility.
     */
    private String guessCategory(
            String showName
    ) {

        String value =
                showName.toLowerCase(
                        Locale.ROOT
                );

        if (
                value.contains("jazz")
                        ||
                value.contains("feya")
                        ||
                value.contains("tutu")
                        ||
                value.contains("pasha")
        ) {
            return "Jazz";
        }

        if (
                value.contains("comedy")
                        ||
                value.contains("gumbi")
                        ||
                value.contains("robvan")
                        ||
                value.contains("rob van")
                        ||
                value.contains("tats")
        ) {
            return "Comedy";
        }

        if (
                value.contains("poetry")
                        ||
                value.contains("poet")
                        ||
                value.contains("folk")
        ) {
            return "Folk";
        }

        if (
                value.contains("hiphop")
                        ||
                value.contains("hip hop")
                        ||
                value.contains("urban")
                        ||
                value.contains("yahkeem")
                        ||
                value.contains("yakheem")
        ) {
            return "HipHop";
        }

        if (
                value.contains("reggae")
                        ||
                value.contains("selassie")
                        ||
                value.contains("survivals")
                        ||
                value.contains("420")
                        ||
                value.contains("dub")
        ) {
            return "Reggae";
        }

        if (
                value.contains("soul")
                        ||
                value.contains("afro")
                        ||
                value.contains("zamajobe")
                        ||
                value.contains("mxo")
                        ||
                value.contains("camagwini")
                        ||
                value.contains("zolani")
                        ||
                value.contains("bongeziwe")
                        ||
                value.contains("ntsika")
                        ||
                value.contains("asanda")
                        ||
                value.contains("brenda")
        ) {
            return "Afrosoul";
        }

        return "Afrosoul";
    }


    private String normaliseCategory(
            String category
    ) {

        if (category == null) {
            return null;
        }

        return switch (
                category.trim()
                        .toLowerCase(
                                Locale.ROOT
                        )
        ) {

            case "afrosoul" ->
                    "Afrosoul";

            case "jazz" ->
                    "Jazz";

            case "comedy" ->
                    "Comedy";

            case "poetry",
                 "folk" ->
                    "Folk";

            case "hiphop" ->
                    "HipHop";

            case "reggae" ->
                    "Reggae";

            default ->
                    null;
        };
    }


    private String buildContactKey(
            String email,
            String phone,
            String ticket,
            String showName,
            int rowNumber
    ) {

        if (!email.isBlank()) {
            return "email:" + email;
        }

        if (!phone.isBlank()) {
            return "phone:" + phone;
        }

        if (!ticket.isBlank()) {
            return "ticket:"
                    + showName
                    + ":"
                    + ticket;
        }

        return "row:"
                + showName
                + ":"
                + rowNumber;
    }


    /**
     * Matches the current legacy South African phone cleanup.
     */
    private String normalisePhone(
            String value
    ) {

        String phone =
                value == null
                        ? ""
                        : value.replaceAll(
                                "\\D",
                                ""
                        );

        if (
                phone.startsWith("27")
                        &&
                phone.length() == 11
        ) {

            return "0"
                    + phone.substring(2);
        }

        return phone;
    }


    private int findIndex(
            List<String> headers,
            String expected
    ) {

        for (
                int index = 0;
                index < headers.size();
                index++
        ) {

            if (
                    headers
                            .get(index)
                            .trim()
                            .equalsIgnoreCase(
                                    expected
                            )
            ) {
                return index;
            }
        }

        return -1;
    }


    private int findIndexAny(
            List<String> headers,
            String... expectedNames
    ) {

        return Arrays
                .stream(expectedNames)
                .mapToInt(
                        expected ->
                                findIndex(
                                        headers,
                                        expected
                                )
                )
                .filter(index ->
                        index >= 0
                )
                .findFirst()
                .orElse(-1);
    }


    private String valueAt(
            List<String> row,
            int index
    ) {

        if (
                index < 0
                        ||
                index >= row.size()
        ) {
            return "";
        }

        return row.get(index);
    }


    /**
     * Supports both comma and semicolon CSV exports.
     */
    private char detectDelimiter(
            String header
    ) {

        long commas =
                header.chars()
                        .filter(character ->
                                character == ','
                        )
                        .count();

        long semicolons =
                header.chars()
                        .filter(character ->
                                character == ';'
                        )
                        .count();

        return semicolons > commas
                ? ';'
                : ',';
    }


    /**
     * Minimal RFC-4180-style CSV parser supporting:
     *
     * - quoted fields;
     * - embedded delimiters;
     * - escaped double quotes.
     */
    private List<String> parseCsvLine(
            String line,
            char delimiter
    ) {

        List<String> values =
                new ArrayList<>();

        StringBuilder current =
                new StringBuilder();

        boolean quoted =
                false;

        for (
                int index = 0;
                index < line.length();
                index++
        ) {

            char character =
                    line.charAt(index);

            if (character == '"') {

                if (
                        quoted
                                &&
                        index + 1
                                < line.length()
                                &&
                        line.charAt(index + 1)
                                == '"'
                ) {

                    current.append('"');

                    index++;

                } else {

                    quoted =
                            !quoted;
                }

                continue;
            }

            if (
                    character == delimiter
                            &&
                    !quoted
            ) {

                values.add(
                        current.toString()
                );

                current.setLength(0);

                continue;
            }

            current.append(
                    character
            );
        }

        values.add(
                current.toString()
        );

        return values;
    }


    private String removeCsvExtension(
            String filename
    ) {

        if (
                filename.toLowerCase(
                        Locale.ROOT
                ).endsWith(".csv")
        ) {

            return filename.substring(
                    0,
                    filename.length() - 4
            );
        }

        return filename;
    }


    /**
     * Mutable internal processing object.
     *
     * This never leaves the service layer.
     */
    private static final class ContactAccumulator {

        private String email = "";
        private String phone = "";

        private final Set<String> shows =
                new LinkedHashSet<>();

        void setEmailIfMissing(
                String value
        ) {

            if (email.isBlank()) {
                email = value;
            }
        }

        void setPhoneIfMissing(
                String value
        ) {

            if (phone.isBlank()) {
                phone = value;
            }
        }

        Set<String> shows() {
            return shows;
        }
    }


    /**
     * Internal result for one processed event CSV.
     */
    private record ShowProcessingResult(
            Set<String> emails,
            Set<String> phones,
            int ticketCount
    ) {
    }
}
