package com.eventpulse.domain.analytics;

import java.util.List;
import java.util.Set;

public class GraphAnalysis {

    private final List<Set<Integer>> showToContacts;
    private final List<Set<Integer>> contactToShows;
    private final double[][] jaccardMatrix;
    private final int[] showDegree;
    private final List<List<Integer>> contactComponents;
    private final int crossGenreFans;
    private final int[] showBridgeness;

    public GraphAnalysis(
            List<Set<Integer>> showToContacts,
            List<Set<Integer>> contactToShows,
            double[][] jaccardMatrix,
            int[] showDegree,
            List<List<Integer>> contactComponents,
            int crossGenreFans,
            int[] showBridgeness) {

        this.showToContacts = showToContacts;
        this.contactToShows = contactToShows;
        this.jaccardMatrix = jaccardMatrix;
        this.showDegree = showDegree;
        this.contactComponents = contactComponents;
        this.crossGenreFans = crossGenreFans;
        this.showBridgeness = showBridgeness;
    }

    public List<Set<Integer>> getShowToContacts() {
        return showToContacts;
    }

    public List<Set<Integer>> getContactToShows() {
        return contactToShows;
    }

    public double[][] getJaccardMatrix() {
        return jaccardMatrix;
    }

    public int[] getShowDegree() {
        return showDegree;
    }

    public List<List<Integer>> getContactComponents() {
        return contactComponents;
    }

    public int getCrossGenreFans() {
        return crossGenreFans;
    }

    public int[] getShowBridgeness() {
        return showBridgeness;
    }
}
