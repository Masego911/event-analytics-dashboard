package com.eventpulse.domain.analytics;

import java.util.List;

public record DynamicProgrammingResult(
        List<String> optimalShows,
        int capacityUsed,
        int expectedAudience,
        List<AudienceSimilarity> audienceSimilarities
) {

    public record AudienceSimilarity(
            String showA,
            String showB,
            int sharedAudienceDepth
    ) {
    }
}
