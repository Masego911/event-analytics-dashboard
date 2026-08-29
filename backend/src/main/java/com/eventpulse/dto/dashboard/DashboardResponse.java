package com.eventpulse.dto.dashboard;

import java.util.List;

/**
 * Response contract consumed by Dashboard.jsx.
 */
public record DashboardResponse(
        DashboardSummary summary,
        List<ShowSummary> shows,
        List<CategorySummary> categories
) {
}
