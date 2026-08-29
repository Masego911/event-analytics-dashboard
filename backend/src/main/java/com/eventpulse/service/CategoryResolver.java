package com.eventpulse.service;

import java.util.Locale;
import java.util.Map;

/** Preserves the category inference and override rules from the original importer. */
public final class CategoryResolver {
    public String resolve(String showName, Map<String, String> overrides) {
        String override = overrides.get(showName.toLowerCase(Locale.ROOT));
        return override != null ? override : infer(showName);
    }

    public String infer(String showName) {
        String s = showName.toLowerCase(Locale.ROOT);
        if (containsAny(s, "jazz", "feya", "tutu", "pasha")) return "Jazz";
        if (containsAny(s, "comedy", "gumbi", "robvan", "rob van", "tats")) return "Comedy";
        if (containsAny(s, "poetry", "poet", "folk")) return "Folk";
        if (containsAny(s, "hiphop", "hip hop", "urban", "yahkeem", "yakheem")) return "HipHop";
        if (containsAny(s, "reggae", "selassie", "survivals", "420", "dub")) return "Reggae";
        return "Afrosoul";
    }

    public String normalize(String category) {
        if (category == null) return null;
        return switch (category.trim().toLowerCase(Locale.ROOT)) {
            case "afrosoul" -> "Afrosoul";
            case "jazz" -> "Jazz";
            case "comedy" -> "Comedy";
            case "poetry", "folk" -> "Folk";
            case "hiphop" -> "HipHop";
            case "reggae" -> "Reggae";
            default -> null;
        };
    }

    private boolean containsAny(String value, String... terms) {
        for (String term : terms) if (value.contains(term)) return true;
        return false;
    }
}
