package com.eventpulse.dto.dashboard;

/**
 * Aggregated audience reach for one EventPulse category.
 */
public record CategorySummary(
        String name,
        int emailCount,
        int phoneCount
) {
}
