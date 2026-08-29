package com.eventpulse.domain;

import java.util.List;

/** Complete imported audience snapshot shared by dashboard analytics. */
public record AudienceData(
        List<String> showNames,
        List<List<String>> showEmails,
        List<List<String>> showPhones,
        List<String> contactEmails,
        List<String> contactPhones,
        List<List<String>> contactShows,
        List<String> firstNames,
        List<String> surnames,
        List<Integer> showTicketCounts,
        int[] categoryTicketCounts,
        int totalTickets,
        int[] timeBuckets,
        int[] groupSizeBuckets
) {
    public AudienceData {
        showNames = List.copyOf(showNames);
        showEmails = List.copyOf(showEmails);
        showPhones = List.copyOf(showPhones);
        contactEmails = List.copyOf(contactEmails);
        contactPhones = List.copyOf(contactPhones);
        contactShows = List.copyOf(contactShows);
        firstNames = List.copyOf(firstNames);
        surnames = List.copyOf(surnames);
        showTicketCounts = List.copyOf(showTicketCounts);
        categoryTicketCounts = categoryTicketCounts.clone();
        timeBuckets = timeBuckets.clone();
        groupSizeBuckets = groupSizeBuckets.clone();
    }
}
