package com.eventpulse.importer;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.service.CategoryService;
import com.eventpulse.service.DataSettingsService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@Service
public class EventCsvImporter {

    private final Path dataDirectory;

    private final CsvParser csvParser;
    private final CategoryService categoryService;
    private final DataSettingsService settingsService;

    private final Object cacheLock =
            new Object();

    private volatile String cachedRevision;

    private volatile AudienceData cachedData;

    public EventCsvImporter(
            @Value("${eventpulse.data.directory:../..}")
            String dataDirectory,

            CsvParser csvParser,
            CategoryService categoryService,
            DataSettingsService settingsService) {

        this.dataDirectory =
                Path.of(dataDirectory)
                        .toAbsolutePath()
                        .normalize();

        this.csvParser =
                csvParser;

        this.categoryService =
                categoryService;

        this.settingsService =
                settingsService;
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    /*
     * Equivalent of legacy processFolder().
     */
    public AudienceData load() {

        String revision =
                calculateRevision();

        AudienceData current =
                cachedData;

        if (current != null
                && revision.equals(
                        cachedRevision
                )) {

            return current;
        }

        synchronized (cacheLock) {

            revision =
                    calculateRevision();

            if (cachedData != null
                    && revision.equals(
                            cachedRevision
                    )) {

                return cachedData;
            }

            AudienceData loaded =
                    importData();

            cachedData =
                    loaded;

            cachedRevision =
                    revision;

            return loaded;
        }
    }

    public void invalidateCache() {

        synchronized (cacheLock) {

            cachedData = null;
            cachedRevision = null;
        }
    }

    /*
     * Equivalent of legacy loadFolder().
     */
    private AudienceData importData() {

        AudienceData data =
                new AudienceData();

        Set<String> excludedEmails =
                settingsService
                        .loadExcludedEmails(
                                dataDirectory
                        );

        Map<String, String> overrides =
                settingsService
                        .loadCategoryOverrides(
                                dataDirectory
                        );

        Map<String, Integer> contactIndexes =
                new LinkedHashMap<>();

        Map<String, Integer> orderCounts =
                new LinkedHashMap<>();

        List<Path> csvFiles =
                findCsvFiles();

        for (Path csvFile : csvFiles) {

            importShow(
                    csvFile,
                    data,
                    excludedEmails,
                    overrides,
                    contactIndexes,
                    orderCounts
            );
        }

        for (Integer count :
                orderCounts.values()) {

            if (count == null
                    || count <= 0) {

                continue;
            }

            data.getGroupSizeBuckets()
                    [Math.min(count, 6)]++;
        }

        writeContactsWithShows(data);

        return data;
    }

    private void importShow(
            Path csvFile,
            AudienceData data,
            Set<String> excludedEmails,
            Map<String, String> overrides,
            Map<String, Integer> contactIndexes,
            Map<String, Integer> orderCounts) {

        String fileName =
                csvFile
                        .getFileName()
                        .toString();

        String showName =
                fileName.substring(
                        0,
                        fileName.length() - 4
                );

        String category =
                categoryService.categoryForShow(
                        showName,
                        overrides
                );

        int categoryIndex =
                categoryService.categoryIndex(
                        category
                );

        data.getShowNames()
                .add(showName);

        data.getShowCategories()
                .add(category);

        List<String> showEmails =
                new ArrayList<>();

        List<String> showPhones =
                new ArrayList<>();

        data.getShowEmails()
                .add(showEmails);

        data.getShowPhones()
                .add(showPhones);

        data.getShowTicketCounts()
                .add(0);

        int showIndex =
                data.getShowNames().size() - 1;

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             csvFile,
                             StandardCharsets.UTF_8
                     )) {

            String header =
                    reader.readLine();

            if (header == null) {
                return;
            }

            char delimiter =
                    csvParser.detectDelimiter(
                            header
                    );

            List<String> columns =
                    csvParser.parseLine(
                            header,
                            delimiter
                    );

            int emailColumn =
                    csvParser.findColumn(
                            columns,
                            "Email"
                    );

            int purchaserEmailColumn =
                    csvParser.findColumn(
                            columns,
                            "Purchaser Email"
                    );

            int phoneColumn =
                    csvParser.findAnyColumn(
                            columns,
                            "Cellphone",
                            "Phone"
                    );

            int ticketColumn =
                    csvParser.findAnyColumn(
                            columns,
                            "Ticket Number",
                            "Barcode"
                    );

            int orderColumn =
                    csvParser.findColumn(
                            columns,
                            "Order Number"
                    );

            int paymentReferenceColumn =
                    csvParser.findColumn(
                            columns,
                            "Payment Reference"
                    );

            int firstNameColumn =
                    csvParser.findColumn(
                            columns,
                            "First name"
                    );

            int surnameColumn =
                    csvParser.findColumn(
                            columns,
                            "Surname"
                    );

            String line;
            int rowNumber = 0;

            while ((line = reader.readLine()) != null) {

                rowNumber++;

                if (line.isBlank()) {
                    continue;
                }

                List<String> row =
                        csvParser.parseLine(
                                line,
                                delimiter
                        );

                String email =
                        normaliseEmail(
                                csvParser.value(
                                        row,
                                        emailColumn
                                )
                        );

                String purchaserEmail =
                        normaliseEmail(
                                csvParser.value(
                                        row,
                                        purchaserEmailColumn
                                )
                        );

                String phone =
                        normalisePhone(
                                csvParser.value(
                                        row,
                                        phoneColumn
                                )
                        );

                String ticket =
                        csvParser.value(
                                row,
                                ticketColumn
                        );

                /*
                 * Preserve original exclusion behaviour.
                 */
                if ((!email.isEmpty()
                        && excludedEmails.contains(email))
                        ||
                        (!purchaserEmail.isEmpty()
                        && excludedEmails.contains(
                                purchaserEmail
                        ))) {

                    continue;
                }

                String finalEmail =
                        !email.isEmpty()
                                ? email
                                : purchaserEmail;

                String contactKey =
                        createContactKey(
                                showName,
                                rowNumber,
                                email,
                                purchaserEmail,
                                phone,
                                ticket
                        );

                String firstName =
                        csvParser.value(
                                row,
                                firstNameColumn
                        );

                String surname =
                        csvParser.value(
                                row,
                                surnameColumn
                        );

                Integer existingIndex =
                        contactIndexes.get(
                                contactKey
                        );

                if (existingIndex == null) {

                    int newIndex =
                            data.getContactEmails()
                                    .size();

                    contactIndexes.put(
                            contactKey,
                            newIndex
                    );

                    data.getContactEmails()
                            .add(finalEmail);

                    data.getContactPhones()
                            .add(phone);

                    data.getContactFirstNames()
                            .add(firstName);

                    data.getContactSurnames()
                            .add(surname);

                    List<String> attendedShows =
                            new ArrayList<>();

                    attendedShows.add(
                            showName
                    );

                    data.getContactShows()
                            .add(attendedShows);

                } else {

                    mergeContact(
                            data,
                            existingIndex,
                            showName,
                            finalEmail,
                            phone,
                            firstName,
                            surname
                    );
                }

                addUnique(
                        showEmails,
                        finalEmail
                );

                addUnique(
                        showPhones,
                        phone
                );

                data.addCategoryEmail(
                        category,
                        finalEmail
                );

                data.addCategoryPhone(
                        category,
                        phone
                );

                data.incrementTotalTickets();

                data.getShowTicketCounts()
                        .set(
                                showIndex,
                                data.getShowTicketCounts()
                                        .get(showIndex) + 1
                        );

                if (categoryIndex >= 1
                        && categoryIndex <= 6) {

                    data.getCategoryTicketCounts()
                            [categoryIndex - 1]++;
                }

                /*
                 * Payment Reference first,
                 * Order Number fallback.
                 */
                String orderKey =
                        csvParser.value(
                                row,
                                paymentReferenceColumn
                        );

                if (orderKey.isEmpty()) {

                    orderKey =
                            csvParser.value(
                                    row,
                                    orderColumn
                            );
                }

                if (!orderKey.isEmpty()) {

                    orderCounts.merge(
                            orderKey,
                            1,
                            Integer::sum
                    );
                }

                classifyBookingTime(
                        row,
                        data
                );
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to process event CSV: "
                            + csvFile,
                    exception
            );
        }
    }

    private void mergeContact(
            AudienceData data,
            int contactIndex,
            String showName,
            String email,
            String phone,
            String firstName,
            String surname) {

        if (!email.isEmpty()
                && data.getContactEmails()
                        .get(contactIndex)
                        .isEmpty()) {

            data.getContactEmails()
                    .set(
                            contactIndex,
                            email
                    );
        }

        if (!phone.isEmpty()
                && data.getContactPhones()
                        .get(contactIndex)
                        .isEmpty()) {

            data.getContactPhones()
                    .set(
                            contactIndex,
                            phone
                    );
        }

        if (!firstName.isEmpty()
                && data.getContactFirstNames()
                        .get(contactIndex)
                        .isEmpty()) {

            data.getContactFirstNames()
                    .set(
                            contactIndex,
                            firstName
                    );
        }

        if (!surname.isEmpty()
                && data.getContactSurnames()
                        .get(contactIndex)
                        .isEmpty()) {

            data.getContactSurnames()
                    .set(
                            contactIndex,
                            surname
                    );
        }

        List<String> shows =
                data.getContactShows()
                        .get(contactIndex);

        if (!containsIgnoreCase(
                shows,
                showName)) {

            shows.add(showName);
        }
    }

    private void classifyBookingTime(
            List<String> row,
            AudienceData data) {

        /*
         * Preserve the legacy behaviour:
         * scan the row for the first cell resembling
         * yyyy-MM-dd HH...
         */
        for (String cell : row) {

            if (cell == null) {
                continue;
            }

            String value =
                    cell.trim();

            if (value.length() < 13) {
                continue;
            }

            if (!value
                    .substring(0, 10)
                    .matches(
                            "\\d{4}-\\d{2}-\\d{2}"
                    )) {

                continue;
            }

            if (value.charAt(10) != ' ') {
                continue;
            }

            try {

                int hour =
                        Integer.parseInt(
                                value.substring(
                                        11,
                                        13
                                )
                        );

                if (hour >= 6 && hour <= 11) {

                    data.getBookingTimeBuckets()[0]++;

                } else if (hour >= 12
                        && hour <= 16) {

                    data.getBookingTimeBuckets()[1]++;

                } else if (hour >= 17
                        && hour <= 21) {

                    data.getBookingTimeBuckets()[2]++;

                } else {

                    data.getBookingTimeBuckets()[3]++;
                }

            } catch (NumberFormatException ignored) {
            }

            break;
        }
    }

    private String createContactKey(
            String showName,
            int rowNumber,
            String email,
            String purchaserEmail,
            String phone,
            String ticket) {

        if (!email.isEmpty()) {
            return "email:" + email;
        }

        if (!purchaserEmail.isEmpty()) {
            return "email:" + purchaserEmail;
        }

        if (!phone.isEmpty()) {
            return "phone:" + phone;
        }

        if (!ticket.isEmpty()) {
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

    private String normaliseEmail(
            String value) {

        if (value == null) {
            return "";
        }

        return value
                .trim()
                .toLowerCase(
                        Locale.ROOT
                );
    }

    private String normalisePhone(
            String value) {

        if (value == null) {
            return "";
        }

        String phone =
                value.replaceAll(
                        "\\D",
                        ""
                );

        /*
         * Preserve original South African
         * 27xxxxxxxxx -> 0xxxxxxxxx conversion.
         */
        if (phone.startsWith("27")
                && phone.length() == 11) {

            phone =
                    "0"
                            + phone.substring(2);
        }

        return phone;
    }

    private void addUnique(
            List<String> values,
            String value) {

        if (value == null
                || value.isBlank()) {

            return;
        }

        if (!values.contains(value)) {
            values.add(value);
        }
    }

    private boolean containsIgnoreCase(
            List<String> values,
            String target) {

        for (String value : values) {

            if (value.equalsIgnoreCase(target)) {
                return true;
            }
        }

        return false;
    }

    private List<Path> findCsvFiles() {

        if (!Files.isDirectory(
                dataDirectory)) {

            return List.of();
        }

        try (Stream<Path> paths =
                     Files.list(
                             dataDirectory
                     )) {

            return paths
                    .filter(Files::isRegularFile)

                    .filter(path -> {

                        String name =
                                path
                                        .getFileName()
                                        .toString();

                        return name
                                .toLowerCase(
                                        Locale.ROOT
                                )
                                .endsWith(".csv");
                    })

                    .filter(path -> {

                        String name =
                                path
                                        .getFileName()
                                        .toString();

                        return !name.equalsIgnoreCase(
                                "contacts_with_shows.csv"
                        );
                    })

                    .filter(path -> {

                        String name =
                                path
                                        .getFileName()
                                        .toString();

                        return !name.equalsIgnoreCase(
                                "all_contacts.csv"
                        );
                    })

                    .sorted(
                            Comparator.comparing(
                                    path ->
                                            path
                                                    .getFileName()
                                                    .toString(),
                                    String.CASE_INSENSITIVE_ORDER
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

    private String calculateRevision() {

        if (!Files.isDirectory(
                dataDirectory)) {

            return dataDirectory.toString();
        }

        StringBuilder revision =
                new StringBuilder(
                        dataDirectory.toString()
                );

        try (Stream<Path> paths =
                     Files.list(
                             dataDirectory
                     )) {

            List<Path> inputs =
                    paths
                            .filter(
                                    Files::isRegularFile
                            )

                            .filter(path -> {

                                String name =
                                        path
                                                .getFileName()
                                                .toString();

                                boolean csv =
                                        name
                                                .toLowerCase(
                                                        Locale.ROOT
                                                )
                                                .endsWith(".csv")

                                                && !name.equalsIgnoreCase(
                                                        "contacts_with_shows.csv"
                                                )

                                                && !name.equalsIgnoreCase(
                                                        "all_contacts.csv"
                                                );

                                return csv
                                        || name.equalsIgnoreCase(
                                                "categories.txt"
                                        )
                                        || name.equalsIgnoreCase(
                                                "excluded_emails.txt"
                                        );
                            })

                            .sorted(
                                    Comparator.comparing(
                                            path ->
                                                    path
                                                            .getFileName()
                                                            .toString(),
                                            String.CASE_INSENSITIVE_ORDER
                                    )
                            )

                            .toList();

            for (Path input : inputs) {

                revision
                        .append('|')
                        .append(
                                input
                                        .getFileName()
                                        .toString()
                        )
                        .append(':')
                        .append(
                                Files.size(input)
                        )
                        .append(':')
                        .append(
                                Files
                                        .getLastModifiedTime(
                                                input
                                        )
                                        .toMillis()
                        );
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to calculate EventPulse data revision",
                    exception
            );
        }

        return revision.toString();
    }

    /*
     * Preserve contacts_with_shows.csv export.
     */
    private void writeContactsWithShows(
            AudienceData data) {

        Path output =
                dataDirectory.resolve(
                        "contacts_with_shows.csv"
                );

        try (BufferedWriter writer =
                     Files.newBufferedWriter(
                             output,
                             StandardCharsets.UTF_8
                     )) {

            writer.write(
                    "Email,Number,ShowsAttended"
            );

            writer.newLine();

            for (int i = 0;
                 i < data.getContactEmails().size();
                 i++) {

                String email =
                        data.getContactEmails()
                                .get(i);

                String phone =
                        data.getContactPhones()
                                .get(i);

                List<String> shows =
                        data.getContactShows()
                                .get(i);

                String showsText =
                        shows == null
                                ? ""
                                : String.join(
                                        " | ",
                                        shows
                                );

                writer.write(
                        escapeCsv(email)
                );

                writer.write(',');

                writer.write(
                        escapeCsv(phone)
                );

                writer.write(',');

                writer.write(
                        escapeCsv(showsText)
                );

                writer.newLine();
            }

        } catch (IOException exception) {

            throw new IllegalStateException(
                    "Unable to create contacts_with_shows.csv",
                    exception
            );
        }
    }

    private String escapeCsv(
            String value) {

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
