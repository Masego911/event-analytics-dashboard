package com.eventpulse.domain.analytics;

import java.util.List;

public record GreedyResult(
        List<String> coverSet,
        List<Integer> coverMarginal,
        int totalCovered,
        int totalContacts,
        double coveragePercentage,
        List<String> categoryOrder,
        List<Integer> categoryMarginal
) {
    public static final double TARGET = 0.80;
}
