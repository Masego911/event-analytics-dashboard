package com.eventpulse.service;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Contains the business logic for reporting backend health.
 *
 * The controller delegates to this class rather than building
 * the response itself. This establishes the service layer from
 * the first migrated endpoint.
 */
@Service
public class HealthService {

    public Map<String, Object> getHealthStatus() {

        Map<String, Object> status =
                new LinkedHashMap<>();

        status.put(
                "application",
                "EventPulse AI"
        );

        status.put(
                "status",
                "UP"
        );

        status.put(
                "timestamp",
                Instant.now()
        );

        status.put(
                "architecture",
                "Spring Boot"
        );

        return status;
    }
}
