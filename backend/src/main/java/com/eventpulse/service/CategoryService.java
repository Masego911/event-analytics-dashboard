package com.eventpulse.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class CategoryService {

    public static final List<String> CATEGORIES = List.of(
            "Afrosoul",
            "Jazz",
            "Comedy",
            "Folk",
            "HipHop",
            "Reggae"
    );

    public String categoryForShow(
            String showName,
            Map<String, String> overrides) {

        if (overrides != null) {
            for (Map.Entry<String, String> entry : overrides.entrySet()) {

                if (entry.getKey().equalsIgnoreCase(showName)) {

                    String normalised =
                            normalizeCategory(entry.getValue());

                    if (normalised != null) {
                        return normalised;
                    }
                }
            }
        }

        return categoryGuess(showName);
    }

    public String categoryGuess(String name) {

        if (name == null) {
            return "Afrosoul";
        }

        String s = name.toLowerCase(Locale.ROOT);

        if (s.contains("jazz")
                || s.contains("feya")
                || s.contains("tutu")
                || s.contains("pasha")) {
            return "Jazz";
        }

        if (s.contains("comedy")
                || s.contains("gumbi")
                || s.contains("robvan")
                || s.contains("rob van")
                || s.contains("tats")) {
            return "Comedy";
        }

        if (s.contains("poetry")
                || s.contains("poet")
                || s.contains("folk")) {
            return "Folk";
        }

        if (s.contains("hiphop")
                || s.contains("hip hop")
                || s.contains("urban")
                || s.contains("yahkeem")
                || s.contains("yakheem")) {
            return "HipHop";
        }

        if (s.contains("reggae")
                || s.contains("selassie")
                || s.contains("survivals")
                || s.contains("420")
                || s.contains("dub")) {
            return "Reggae";
        }

        if (s.contains("soul")
                || s.contains("afro")
                || s.contains("zamajobe")
                || s.contains("mxo")
                || s.contains("camagwini")
                || s.contains("zolani")
                || s.contains("bongeziwe")
                || s.contains("ntsika")
                || s.contains("asanda")
                || s.contains("brenda")) {
            return "Afrosoul";
        }

        return "Afrosoul";
    }

    public String normalizeCategory(String category) {

        if (category == null) {
            return null;
        }

        return switch (category.toLowerCase(Locale.ROOT)) {
            case "afrosoul" -> "Afrosoul";
            case "jazz" -> "Jazz";
            case "comedy" -> "Comedy";
            case "poetry", "folk" -> "Folk";
            case "hiphop" -> "HipHop";
            case "reggae" -> "Reggae";
            default -> null;
        };
    }

    public int categoryIndex(String category) {

        if (category == null) {
            return 1;
        }

        return switch (category.toLowerCase(Locale.ROOT)) {
            case "afrosoul" -> 1;
            case "jazz" -> 2;
            case "comedy" -> 3;
            case "folk" -> 4;
            case "hiphop" -> 5;
            case "reggae" -> 6;
            default -> 1;
        };
    }
}
