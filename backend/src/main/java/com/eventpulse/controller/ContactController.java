package com.eventpulse.controller;

import com.eventpulse.domain.AudienceData;
import com.eventpulse.importer.EventCsvImporter;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Contact views backed by the same imported audience snapshot as the dashboard. */
@RestController
@RequestMapping("/api")
public class ContactController {
    private final EventCsvImporter importer;

    public ContactController(EventCsvImporter importer) {
        this.importer = importer;
    }

    @GetMapping("/show")
    public Map<String, Object> show(@RequestParam int i) {
        AudienceData data = importer.load();
        if (i < 0 || i >= data.getShowNames().size()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Show not found");
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("show", data.getShowNames().get(i));
        response.put("emails", data.getShowEmails().get(i));
        List<Map<String, String>> phones = new ArrayList<>();
        for (String phone : data.getShowPhones().get(i)) phones.add(Map.of("name", "", "number", phone));
        response.put("phones", phones);
        return response;
    }

    @GetMapping("/category")
    public Map<String, Object> category(@RequestParam String cat) {
        AudienceData data = importer.load();
        if (!data.getCategoryEmails().containsKey(cat)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Category not found");
        }
        List<Map<String, Object>> contacts = new ArrayList<>();
        for (String email : data.getCategoryEmails(cat)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("email", email);
            int contactIndex = data.getContactEmails().indexOf(email);
            row.put("shows", contactIndex >= 0 ? data.getContactShows().get(contactIndex) : List.of());
            contacts.add(row);
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("category", cat);
        response.put("contacts", contacts);
        List<Map<String, String>> phones = new ArrayList<>();
        for (String phone : data.getCategoryPhones(cat)) phones.add(Map.of("name", "", "number", phone));
        response.put("phones", phones);
        return response;
    }

}
