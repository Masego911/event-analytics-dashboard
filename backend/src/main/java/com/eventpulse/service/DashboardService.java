package com.eventpulse.service;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.dto.dashboard.CategorySummary;
import com.eventpulse.dto.dashboard.DashboardResponse;
import com.eventpulse.dto.dashboard.DashboardSummary;
import com.eventpulse.dto.dashboard.ShowSummary;
import com.eventpulse.importer.EventCsvImporter;
import org.springframework.stereotype.Service;

import java.util.List;

/** Maps the shared imported snapshot to the dashboard API contract. */
@Service
public class DashboardService {
    private static final List<String> CATEGORIES = List.of("Afrosoul", "Jazz", "Comedy", "Folk", "HipHop", "Reggae");
    private final EventCsvImporter importer;

    public DashboardService(EventCsvImporter importer) {
        this.importer = importer;
    }

    public DashboardResponse getDashboard() {
        AudienceData data = importer.load();
        int returning = (int) data.getContactShows().stream().filter(shows -> shows != null && shows.size() > 1).count();
        List<ShowSummary> shows = data.getShowNames().stream().map(name -> {
            int i = data.getShowNames().indexOf(name);
            return new ShowSummary(i, name, data.getShowCategories().get(i), data.getShowEmails().get(i).size(), data.getShowPhones().get(i).size(), data.getShowTicketCounts().get(i));
        }).toList();
        List<CategorySummary> categories = CATEGORIES.stream()
                .map(category -> new CategorySummary(category, data.getCategoryEmails(category).size(), data.getCategoryPhones(category).size()))
                .toList();
        return new DashboardResponse(new DashboardSummary(shows.size(), data.getContactEmails().size(), data.getTotalTickets(), returning), shows, categories);
    }
}
