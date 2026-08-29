package com.eventpulse.domain.analytics;

import java.util.List;

public record StatisticsResult(
        double pearsonCorrelation,
        String pearsonInterpretation,
        double chiSquared,
        double chiSquaredPValue,
        double cohortRetentionRate,
        List<Double> movingAverageRevenue,
        List<String> movingAverageLabels,
        double[] showZScores,
        List<ShowSimilarity> topJaccardPairs
) {

    public record ShowSimilarity(
            String showA,
            String showB,
            double score
    ) {
    }
}
