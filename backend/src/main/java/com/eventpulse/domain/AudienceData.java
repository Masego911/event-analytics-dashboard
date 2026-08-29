package com.eventpulse.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AudienceData {

    private final List<String> showNames =
            new ArrayList<>();

    private final List<String> showCategories =
            new ArrayList<>();

    private final List<List<String>> showEmails =
            new ArrayList<>();

    private final List<List<String>> showPhones =
            new ArrayList<>();

    private final List<String> contactEmails =
            new ArrayList<>();

    private final List<String> contactPhones =
            new ArrayList<>();

    private final List<List<String>> contactShows =
            new ArrayList<>();

    private final List<String> contactFirstNames =
            new ArrayList<>();

    private final List<String> contactSurnames =
            new ArrayList<>();

    private final List<Integer> showTicketCounts =
            new ArrayList<>();

    private final int[] categoryTicketCounts =
            new int[6];

    /*
     * Morning
     * Afternoon
     * Evening
     * Late / Other
     */
    private final int[] bookingTimeBuckets =
            new int[4];

    /*
     * index 0 unused
     * indexes 1-5 exact group size
     * index 6 = six or more
     */
    private final int[] groupSizeBuckets =
            new int[7];

    private final Map<String, Set<String>> categoryEmails =
            new LinkedHashMap<>();

    private final Map<String, Set<String>> categoryPhones =
            new LinkedHashMap<>();

    private int totalTickets;

    public AudienceData() {

        initialiseCategory("Afrosoul");
        initialiseCategory("Jazz");
        initialiseCategory("Comedy");
        initialiseCategory("Folk");
        initialiseCategory("HipHop");
        initialiseCategory("Reggae");
    }

    private void initialiseCategory(String category) {

        categoryEmails.put(
                category,
                new LinkedHashSet<>()
        );

        categoryPhones.put(
                category,
                new LinkedHashSet<>()
        );
    }

    public List<String> getShowNames() {
        return showNames;
    }

    public List<String> getShowCategories() {
        return showCategories;
    }

    public List<List<String>> getShowEmails() {
        return showEmails;
    }

    public List<List<String>> getShowPhones() {
        return showPhones;
    }

    public List<String> getContactEmails() {
        return contactEmails;
    }

    public List<String> getContactPhones() {
        return contactPhones;
    }

    public List<List<String>> getContactShows() {
        return contactShows;
    }

    public List<String> getContactFirstNames() {
        return contactFirstNames;
    }

    public List<String> getContactSurnames() {
        return contactSurnames;
    }

    public List<Integer> getShowTicketCounts() {
        return showTicketCounts;
    }

    public int[] getCategoryTicketCounts() {
        return categoryTicketCounts;
    }

    public int[] getBookingTimeBuckets() {
        return bookingTimeBuckets;
    }

    public int[] getGroupSizeBuckets() {
        return groupSizeBuckets;
    }

    public int getTotalTickets() {
        return totalTickets;
    }

    public void incrementTotalTickets() {
        totalTickets++;
    }

    public Map<String, Set<String>> getCategoryEmails() {
        return categoryEmails;
    }

    public Map<String, Set<String>> getCategoryPhones() {
        return categoryPhones;
    }

    public Set<String> getCategoryEmails(
            String category) {

        return categoryEmails.getOrDefault(
                category,
                Set.of()
        );
    }

    public Set<String> getCategoryPhones(
            String category) {

        return categoryPhones.getOrDefault(
                category,
                Set.of()
        );
    }

    public void addCategoryEmail(
            String category,
            String email) {

        Set<String> values =
                categoryEmails.get(category);

        if (values != null
                && email != null
                && !email.isBlank()) {

            values.add(email);
        }
    }

    public void addCategoryPhone(
            String category,
            String phone) {

        Set<String> values =
                categoryPhones.get(category);

        if (values != null
                && phone != null
                && !phone.isBlank()) {

            values.add(phone);
        }
    }
}
