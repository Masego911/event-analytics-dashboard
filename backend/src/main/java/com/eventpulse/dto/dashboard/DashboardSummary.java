package com.eventpulse.dto.dashboard;

/**
 * Top-level statistics displayed by the React dashboard.
 */
public record DashboardSummary(
        int totalShows,
        int totalContacts,
        int totalTickets,
        int returningAudience
) {
}
