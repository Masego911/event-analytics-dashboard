package com.eventpulse.dto.dashboard;

/**
 * Summary information for one event/show.
 *
 * The field names intentionally match the existing React
 * application contract.
 */
public record ShowSummary(
        int id,
        String name,
        String category,
        int emailCount,
        int phoneCount,
        int ticketCount
) {
}
