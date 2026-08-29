package com.eventpulse.analytics;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.domain.analytics.GraphAnalysis;
import com.eventpulse.domain.analytics.StatisticsResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class StatisticsService {

    public StatisticsResult analyse(
            AudienceData data,
            GraphAnalysis graph,
            List<String> movingAverageLabels,
            List<Double> movingAverageRevenue) {

        int showCount =
                data.getShowNames().size();

        double pearsonCorrelation;
        String pearsonInterpretation;

        if (showCount >= 3) {

            double[] tickets =
                    new double[showCount];

            double[] audience =
                    new double[showCount];

            for (int i = 0; i < showCount; i++) {

                tickets[i] =
                        data.getShowTicketCounts().get(i);

                audience[i] =
                        graph.getShowToContacts()
                                .get(i)
                                .size();
            }

            pearsonCorrelation =
                    pearson(tickets, audience);

            pearsonInterpretation =
                    interpretPearson(
                            pearsonCorrelation
                    );

        } else {

            pearsonCorrelation =
                    Double.NaN;

            pearsonInterpretation =
                    "need ≥3 shows";
        }

        int totalBookings = 0;

        for (int value :
                data.getBookingTimeBuckets()) {

            totalBookings += value;
        }

        double expected =
                totalBookings / 4.0;

        double chiSquared = 0;

        for (int value :
                data.getBookingTimeBuckets()) {

            double difference =
                    value - expected;

            chiSquared +=
                    difference * difference
                            / Math.max(expected, 1);
        }

        double chiSquaredPValue =
                chiSquaredPDf3(chiSquared);

        int returning = 0;
        int totalContacts = 0;

        for (List<String> shows :
                data.getContactShows()) {

            if (shows == null || shows.isEmpty()) {
                continue;
            }

            totalContacts++;

            if (shows.size() > 1) {
                returning++;
            }
        }

        double retention =
                totalContacts == 0
                        ? 0
                        : returning * 100.0 / totalContacts;

        double[] zScores =
                calculateZScores(
                        data.getShowTicketCounts()
                );

        List<StatisticsResult.ShowSimilarity> pairs =
                calculateTopJaccardPairs(
                        data,
                        graph
                );

        return new StatisticsResult(
                pearsonCorrelation,
                pearsonInterpretation,
                chiSquared,
                chiSquaredPValue,
                retention,
                movingAverageRevenue,
                movingAverageLabels,
                zScores,
                pairs
        );
    }

    public double pearson(
            double[] x,
            double[] y) {

        int n = x.length;

        double meanX = 0;
        double meanY = 0;

        for (int i = 0; i < n; i++) {
            meanX += x[i];
            meanY += y[i];
        }

        meanX /= n;
        meanY /= n;

        double numerator = 0;
        double xSquared = 0;
        double ySquared = 0;

        for (int i = 0; i < n; i++) {

            double dx =
                    x[i] - meanX;

            double dy =
                    y[i] - meanY;

            numerator += dx * dy;
            xSquared += dx * dx;
            ySquared += dy * dy;
        }

        double denominator =
                Math.sqrt(
                        xSquared * ySquared
                );

        return denominator == 0
                ? 0
                : numerator / denominator;
    }

    public double chiSquaredPDf3(double value) {

        if (value < 0.352) return 0.95;
        if (value < 0.584) return 0.90;
        if (value < 1.213) return 0.75;
        if (value < 2.366) return 0.50;
        if (value < 4.108) return 0.25;
        if (value < 6.251) return 0.10;
        if (value < 7.815) return 0.05;
        if (value < 9.348) return 0.025;
        if (value < 11.345) return 0.01;

        return 0.001;
    }

    private double[] calculateZScores(
            List<Integer> ticketCounts) {

        int size =
                ticketCounts.size();

        double[] zScores =
                new double[size];

        if (size <= 1) {
            return zScores;
        }

        double mean = 0;

        for (Integer value : ticketCounts) {
            mean += value;
        }

        mean /= size;

        double variance = 0;

        for (Integer value : ticketCounts) {

            double difference =
                    value - mean;

            variance +=
                    difference * difference;
        }

        double standardDeviation =
                Math.sqrt(
                        variance / size
                );

        for (int i = 0; i < size; i++) {

            zScores[i] =
                    standardDeviation == 0
                            ? 0
                            : (
                                ticketCounts.get(i)
                                - mean
                              )
                              / standardDeviation;
        }

        return zScores;
    }

    private List<StatisticsResult.ShowSimilarity>
    calculateTopJaccardPairs(
            AudienceData data,
            GraphAnalysis graph) {

        List<StatisticsResult.ShowSimilarity> pairs =
                new ArrayList<>();

        int showCount =
                data.getShowNames().size();

        for (int first = 0;
             first < showCount;
             first++) {

            for (int second = first + 1;
                 second < showCount;
                 second++) {

                pairs.add(
                        new StatisticsResult.ShowSimilarity(
                                data.getShowNames().get(first),
                                data.getShowNames().get(second),
                                graph.getJaccardMatrix()
                                        [first][second]
                        )
                );
            }
        }

        pairs.sort(
                Comparator
                        .comparingDouble(
                                StatisticsResult.ShowSimilarity::score
                        )
                        .reversed()
        );

        return pairs.size() <= 6
                ? pairs
                : new ArrayList<>(
                        pairs.subList(0, 6)
                );
    }

    private String interpretPearson(
            double correlation) {

        double absolute =
                Math.abs(correlation);

        String direction =
                correlation >= 0
                        ? "positive"
                        : "negative";

        if (absolute >= 0.9)
            return "very strong " + direction;

        if (absolute >= 0.7)
            return "strong " + direction;

        if (absolute >= 0.5)
            return "moderate " + direction;

        if (absolute >= 0.3)
            return "weak " + direction;

        return "negligible correlation";
    }
}
