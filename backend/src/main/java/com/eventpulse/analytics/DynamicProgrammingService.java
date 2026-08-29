package com.eventpulse.analytics;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.domain.analytics.DynamicProgrammingResult;
import com.eventpulse.domain.analytics.GraphAnalysis;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DynamicProgrammingService {

    public DynamicProgrammingResult optimise(
            AudienceData data,
            GraphAnalysis graph) {

        int showCount =
                data.getShowNames().size();

        int capacity =
                Math.max(
                        1,
                        showCount / 2
                );

        int[] values =
                new int[showCount];

        for (int i = 0;
             i < showCount;
             i++) {

            values[i] =
                    graph.getShowToContacts()
                            .get(i)
                            .size();
        }

        /*
         * 0/1 Knapsack.
         */
        int[][] dp =
                new int[
                        showCount + 1
                        ][
                        capacity + 1
                        ];

        for (int i = 1;
             i <= showCount;
             i++) {

            for (int currentCapacity = 0;
                 currentCapacity <= capacity;
                 currentCapacity++) {

                dp[i][currentCapacity] =
                        dp[i - 1][currentCapacity];

                if (currentCapacity >= 1) {

                    dp[i][currentCapacity] =
                            Math.max(
                                    dp[i][currentCapacity],

                                    dp[i - 1]
                                            [currentCapacity - 1]
                                            + values[i - 1]
                            );
                }
            }
        }

        List<String> optimalShows =
                new ArrayList<>();

        int capacityUsed = 0;

        int currentCapacity =
                capacity;

        for (int i = showCount;
             i >= 1;
             i--) {

            if (dp[i][currentCapacity]
                    != dp[i - 1][currentCapacity]) {

                optimalShows.add(
                        data.getShowNames()
                                .get(i - 1)
                );

                capacityUsed++;

                currentCapacity--;
            }
        }

        Collections.reverse(
                optimalShows
        );

        /*
         * LCS audience similarity.
         */
        List<SimilarityCandidate> candidates =
                new ArrayList<>();

        for (int first = 0;
             first < showCount;
             first++) {

            for (int second = first + 1;
                 second < showCount;
                 second++) {

                List<Integer> firstAudience =
                        new ArrayList<>(
                                graph.getShowToContacts()
                                        .get(first)
                        );

                List<Integer> secondAudience =
                        new ArrayList<>(
                                graph.getShowToContacts()
                                        .get(second)
                        );

                Collections.sort(firstAudience);
                Collections.sort(secondAudience);

                int lcsLength =
                        lcs(
                                firstAudience,
                                secondAudience
                        );

                candidates.add(
                        new SimilarityCandidate(
                                first,
                                second,
                                lcsLength
                        )
                );
            }
        }

        candidates.sort(
                Comparator
                        .comparingInt(
                                SimilarityCandidate::score
                        )
                        .reversed()
        );

        List<DynamicProgrammingResult.AudienceSimilarity>
                similarities =
                new ArrayList<>();

        int maximum =
                Math.min(
                        5,
                        candidates.size()
                );

        for (int i = 0;
             i < maximum;
             i++) {

            SimilarityCandidate candidate =
                    candidates.get(i);

            similarities.add(
                    new DynamicProgrammingResult
                            .AudienceSimilarity(

                            data.getShowNames()
                                    .get(candidate.firstShow()),

                            data.getShowNames()
                                    .get(candidate.secondShow()),

                            candidate.score()
                    )
            );
        }

        return new DynamicProgrammingResult(
                optimalShows,
                capacityUsed,
                dp[showCount][capacity],
                similarities
        );
    }

    /*
     * Longest Common Subsequence.
     *
     * Preserves the original 200-item cap.
     */
    public int lcs(
            List<Integer> first,
            List<Integer> second) {

        int m =
                Math.min(
                        first.size(),
                        200
                );

        int n =
                Math.min(
                        second.size(),
                        200
                );

        int[][] dp =
                new int[m + 1][n + 1];

        for (int i = 1;
             i <= m;
             i++) {

            for (int j = 1;
                 j <= n;
                 j++) {

                if (first.get(i - 1)
                        .equals(
                                second.get(j - 1)
                        )) {

                    dp[i][j] =
                            dp[i - 1][j - 1] + 1;

                } else {

                    dp[i][j] =
                            Math.max(
                                    dp[i - 1][j],
                                    dp[i][j - 1]
                            );
                }
            }
        }

        return dp[m][n];
    }

    private record SimilarityCandidate(
            int firstShow,
            int secondShow,
            int score
    ) {
    }
}
