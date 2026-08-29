package com.eventpulse.domain.analytics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class FinancialAnalysis {

    private double totalRevenue;

    private final double[] categoryRevenue =
            new double[6];

    private final Map<String, Double> yearlyRevenue =
            new TreeMap<>();

    private final Map<Integer, Double> monthlyRevenue =
            new TreeMap<>();

    private final Map<String, Double> seasonalRevenue =
            new LinkedHashMap<>();

    private int checkedIn;
    private int totalRows;

    public FinancialAnalysis() {

        seasonalRevenue.put(
                "Summer",
                0.0
        );

        seasonalRevenue.put(
                "Autumn",
                0.0
        );

        seasonalRevenue.put(
                "Winter",
                0.0
        );

        seasonalRevenue.put(
                "Spring",
                0.0
        );
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public void addRevenue(double value) {
        totalRevenue += value;
    }

    public double[] getCategoryRevenue() {
        return categoryRevenue;
    }

    public Map<String, Double> getYearlyRevenue() {
        return yearlyRevenue;
    }

    public Map<Integer, Double> getMonthlyRevenue() {
        return monthlyRevenue;
    }

    public Map<String, Double> getSeasonalRevenue() {
        return seasonalRevenue;
    }

    public int getCheckedIn() {
        return checkedIn;
    }

    public void incrementCheckedIn() {
        checkedIn++;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public void incrementTotalRows() {
        totalRows++;
    }
}
