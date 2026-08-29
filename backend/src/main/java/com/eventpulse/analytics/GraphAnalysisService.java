package com.eventpulse.analytics;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.domain.analytics.GraphAnalysis;
import com.eventpulse.service.CategoryService;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class GraphAnalysisService {

    private final CategoryService categoryService;

    public GraphAnalysisService(CategoryService categoryService) {
        this.categoryService = categoryService;
    }

    public GraphAnalysis analyse(
            AudienceData data,
            Map<String, String> categoryOverrides) {

        int showCount = data.getShowNames().size();
        int contactCount = data.getContactEmails().size();

        List<Set<Integer>> showToContacts =
                new ArrayList<>();

        List<Set<Integer>> contactToShows =
                new ArrayList<>();

        int[] showDegree =
                new int[showCount];

        for (int i = 0; i < showCount; i++) {
            showToContacts.add(new HashSet<>());
        }

        for (int i = 0; i < contactCount; i++) {
            contactToShows.add(new HashSet<>());
        }

        /*
         * Bipartite contact <-> show graph.
         */
        for (int contactIndex = 0;
             contactIndex < contactCount;
             contactIndex++) {

            List<String> attended =
                    data.getContactShows()
                            .get(contactIndex);

            if (attended == null) {
                continue;
            }

            for (String showName : attended) {

                int showIndex =
                        indexOfIgnoreCase(
                                data.getShowNames(),
                                showName
                        );

                if (showIndex >= 0) {

                    showToContacts
                            .get(showIndex)
                            .add(contactIndex);

                    contactToShows
                            .get(contactIndex)
                            .add(showIndex);

                    showDegree[showIndex]++;
                }
            }
        }

        /*
         * Jaccard similarity matrix.
         */
        double[][] jaccardMatrix =
                new double[showCount][showCount];

        for (int first = 0;
             first < showCount;
             first++) {

            jaccardMatrix[first][first] = 1.0;

            for (int second = first + 1;
                 second < showCount;
                 second++) {

                Set<Integer> firstAudience =
                        showToContacts.get(first);

                Set<Integer> secondAudience =
                        showToContacts.get(second);

                if (firstAudience.isEmpty()
                        && secondAudience.isEmpty()) {
                    continue;
                }

                int intersection = 0;

                for (int contact : firstAudience) {

                    if (secondAudience.contains(contact)) {
                        intersection++;
                    }
                }

                int union =
                        firstAudience.size()
                                + secondAudience.size()
                                - intersection;

                double similarity =
                        union == 0
                                ? 0
                                : (double) intersection / union;

                jaccardMatrix[first][second] =
                        similarity;

                jaccardMatrix[second][first] =
                        similarity;
            }
        }

        /*
         * BFS connected components across
         * the contact projection.
         */
        List<List<Integer>> components =
                new ArrayList<>();

        boolean[] visited =
                new boolean[contactCount];

        for (int start = 0;
             start < contactCount;
             start++) {

            if (visited[start]
                    || contactToShows.get(start).isEmpty()) {
                continue;
            }

            List<Integer> component =
                    new ArrayList<>();

            Queue<Integer> queue =
                    new LinkedList<>();

            queue.add(start);
            visited[start] = true;

            while (!queue.isEmpty()) {

                int contactIndex =
                        queue.poll();

                component.add(contactIndex);

                for (int showIndex :
                        contactToShows.get(contactIndex)) {

                    for (int neighbour :
                            showToContacts.get(showIndex)) {

                        if (!visited[neighbour]) {

                            visited[neighbour] = true;

                            queue.add(neighbour);
                        }
                    }
                }
            }

            if (component.size() > 1) {
                components.add(component);
            }
        }

        /*
         * Cross-genre fans.
         */
        int crossGenreFans = 0;

        for (int contactIndex = 0;
             contactIndex < contactCount;
             contactIndex++) {

            List<String> attended =
                    data.getContactShows()
                            .get(contactIndex);

            if (attended == null) {
                continue;
            }

            Set<String> categories =
                    new HashSet<>();

            for (String show : attended) {

                categories.add(
                        categoryService.categoryForShow(
                                show,
                                categoryOverrides
                        )
                );
            }

            if (categories.size() >= 2) {
                crossGenreFans++;
            }
        }

        /*
         * Bridgeness proxy.
         */
        int[] showBridgeness =
                new int[showCount];

        for (int contactIndex = 0;
             contactIndex < contactCount;
             contactIndex++) {

            List<Integer> shows =
                    new ArrayList<>(
                            contactToShows.get(contactIndex)
                    );

            for (int first = 0;
                 first < shows.size();
                 first++) {

                for (int second = first + 1;
                     second < shows.size();
                     second++) {

                    showBridgeness[
                            shows.get(first)
                            ]++;

                    showBridgeness[
                            shows.get(second)
                            ]++;
                }
            }
        }

        return new GraphAnalysis(
                showToContacts,
                contactToShows,
                jaccardMatrix,
                showDegree,
                components,
                crossGenreFans,
                showBridgeness
        );
    }

    private int indexOfIgnoreCase(
            List<String> values,
            String target) {

        for (int i = 0; i < values.size(); i++) {

            if (values.get(i).equalsIgnoreCase(target)) {
                return i;
            }
        }

        return -1;
    }
}
