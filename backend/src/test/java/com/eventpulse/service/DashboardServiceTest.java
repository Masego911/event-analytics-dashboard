package com.eventpulse.service;

import com.eventpulse.dto.dashboard.DashboardResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the dashboard migration against controlled CSV fixtures.
 *
 * No real customer data is used in automated tests.
 */
class DashboardServiceTest {

    @TempDir
    Path tempDirectory;


    @Test
    void dashboardShouldAggregateShowsContactsAndReturningAudience()
            throws Exception {

        Files.writeString(
                tempDirectory.resolve(
                        "Jazz Night.csv"
                ),
                """
                Ticket Number,Purchaser Email,Cellphone
                T1,person1@example.com,0821111111
                T2,person2@example.com,0822222222
                """,
                StandardCharsets.UTF_8
        );

        Files.writeString(
                tempDirectory.resolve(
                        "Comedy Night.csv"
                ),
                """
                Ticket Number,Purchaser Email,Cellphone
                T3,person1@example.com,0821111111
                T4,person3@example.com,0823333333
                """,
                StandardCharsets.UTF_8
        );

        DashboardService service =
                new DashboardService(
                        tempDirectory.toString()
                );

        DashboardResponse response =
                service.getDashboard();

        assertEquals(
                2,
                response.summary()
                        .totalShows()
        );

        assertEquals(
                3,
                response.summary()
                        .totalContacts()
        );

        assertEquals(
                4,
                response.summary()
                        .totalTickets()
        );

        assertEquals(
                1,
                response.summary()
                        .returningAudience()
        );

        assertEquals(
                "Comedy",
                response.shows()
                        .get(0)
                        .category()
        );

        assertEquals(
                "Jazz",
                response.shows()
                        .get(1)
                        .category()
        );
    }

    @Test
    void dashboardShouldPreserveCsvOverridesExclusionsAndPhoneNormalisation()
            throws Exception {

        Files.writeString(tempDirectory.resolve("Folk Showcase.csv"),
                "Ticket Number;Email;Cellphone\n"
                        + "T1;\"quoted@example.com\";+27 (82) 111-1111\n"
                        + "T2;excluded@example.com;0822222222\n",
                StandardCharsets.UTF_8);
        Files.writeString(tempDirectory.resolve("Unknown Act.csv"),
                "Ticket Number,Purchaser Email,Phone\n"
                        + "T3,quoted@example.com,27821111111\n"
                        + "T4,second@example.com,0823333333\n",
                StandardCharsets.UTF_8);
        Files.writeString(tempDirectory.resolve("categories.txt"),
                "Unknown Act,Jazz\n", StandardCharsets.UTF_8);
        Files.writeString(tempDirectory.resolve("excluded_emails.txt"),
                "excluded@example.com\n", StandardCharsets.UTF_8);

        DashboardResponse response = new DashboardService(tempDirectory.toString()).getDashboard();

        assertEquals(2, response.summary().totalShows());
        assertEquals(3, response.summary().totalTickets());
        assertEquals(2, response.summary().totalContacts());
        assertEquals(1, response.summary().returningAudience());
        assertEquals("Folk", response.shows().get(0).category());
        assertEquals("Jazz", response.shows().get(1).category());
        assertEquals(1, response.shows().get(0).emailCount());
        assertEquals(1, response.shows().get(0).phoneCount());
        assertEquals(2, response.categories().stream()
                .filter(category -> category.name().equals("Jazz"))
                .findFirst().orElseThrow().emailCount());
    }
}
