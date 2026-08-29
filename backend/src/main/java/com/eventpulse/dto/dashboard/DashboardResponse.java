package com.eventpulse.dto.dashboard;

import java.util.List;
import java.util.Objects;

/**
 * Response contract consumed by Dashboard.jsx.
 */
public record DashboardResponse(
        DashboardSummary summary,
        List<ShowSummary> shows,
        List<CategorySummary> categories
) {

    public DashboardResponse {
        summary = Objects.requireNonNull(summary, "summary must not be null");
        shows = List.copyOf(Objects.requireNonNull(shows, "shows must not be null"));
        categories = List.copyOf(Objects.requireNonNull(categories, "categories must not be null"));
    }
}
