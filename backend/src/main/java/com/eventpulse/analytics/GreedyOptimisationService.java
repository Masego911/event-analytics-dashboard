package com.eventpulse.analytics;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.domain.analytics.GraphAnalysis;
import com.eventpulse.domain.analytics.GreedyResult;
import com.eventpulse.service.CategoryService;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class GreedyOptimisationService {

    private final CategoryService categoryService;

    public GreedyOptimisationService(
            CategoryService categoryService) {

        this.categoryService =
                categoryService;
    }

    public GreedyResult optimise(
            AudienceData data,
            GraphAnalysis graph,
            Map<String, String> categoryOverrides) {

        int showCount =
                data.getShowNames().size();

        int contactCount =
                data.getContactEmails().size();

        Set<Integer> covered =
                new HashSet<>();

        Set<Integer> remaining =
                new LinkedHashSet<>();

        for (int show = 0;
             show < showCount;
             show++) {

            remaining.add(show);
        }

        List<String> coverSet =
                new ArrayList<>();

        List<Integer> coverMarginal =
                new ArrayList<>();

        int target =
                (int) Math.ceil(
                        contactCount
                                * GreedyResult.TARGET
                );

        /*
         * Greedy set cover.
         */
        while (covered.size() < target
                && !remaining.isEmpty()) {

            int bestShow = -1;
            int bestGain = 0;

            for (int show : remaining) {

                int gain = 0;

                for (int contact :
                        graph.getShowToContacts()
                                .get(show)) {

                    if (!covered.contains(contact)) {
                        gain++;
                    }
                }

                if (gain > bestGain) {
                    bestGain = gain;
                    bestShow = show;
                }
            }

            if (bestShow < 0
                    || bestGain == 0) {
                break;
            }

            covered.addAll(
                    graph.getShowToContacts()
                            .get(bestShow)
            );

            coverSet.add(
                    data.getShowNames()
                            .get(bestShow)
            );

            coverMarginal.add(bestGain);

            remaining.remove(bestShow);
        }

        double coveragePercentage =
                contactCount == 0
                        ? 0
                        : covered.size()
                          * 100.0
                          / contactCount;

        /*
         * Greedy category reach order.
         */
        Map<String, Set<Integer>> categoryMap =
                new LinkedHashMap<>();

        for (String category :
                CategoryService.CATEGORIES) {

            categoryMap.put(
                    category,
                    new HashSet<>()
            );
        }

        for (int contactIndex = 0;
             contactIndex < contactCount;
             contactIndex++) {

            List<String> shows =
                    data.getContactShows()
                            .get(contactIndex);

            if (shows == null) {
                continue;
            }

            for (String show : shows) {

                String category =
                        categoryService.categoryForShow(
                                show,
                                categoryOverrides
                        );

                Set<Integer> contacts =
                        categoryMap.get(category);

                if (contacts != null) {
                    contacts.add(contactIndex);
                }
            }
        }

        Set<Integer> categoryCovered =
                new HashSet<>();

        Set<String> categoryRemaining =
                new LinkedHashSet<>(
                        CategoryService.CATEGORIES
                );

        List<String> categoryOrder =
                new ArrayList<>();

        List<Integer> categoryMarginal =
                new ArrayList<>();

        while (!categoryRemaining.isEmpty()) {

            String bestCategory = null;
            int bestGain = 0;

            for (String category :
                    categoryRemaining) {

                int gain = 0;

                for (int contact :
                        categoryMap.get(category)) {

                    if (!categoryCovered
                            .contains(contact)) {

                        gain++;
                    }
                }

                if (gain > bestGain) {

                    bestGain = gain;
                    bestCategory = category;
                }
            }

            if (bestCategory == null
                    || bestGain == 0) {
                break;
            }

            categoryCovered.addAll(
                    categoryMap.get(
                            bestCategory
                    )
            );

            categoryOrder.add(
                    bestCategory
            );

            categoryMarginal.add(
                    bestGain
            );

            categoryRemaining.remove(
                    bestCategory
            );
        }

        return new GreedyResult(
                coverSet,
                coverMarginal,
                covered.size(),
                contactCount,
                coveragePercentage,
                categoryOrder,
                categoryMarginal
        );
    }
}
