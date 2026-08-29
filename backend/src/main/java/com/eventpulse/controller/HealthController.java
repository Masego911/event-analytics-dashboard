package com.eventpulse.controller;

import com.eventpulse.dto.ApiResponse;
import com.eventpulse.service.HealthService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST controller for infrastructure-level API checks.
 *
 * Controller responsibilities are intentionally limited to:
 *
 * 1. receiving HTTP requests;
 * 2. delegating work to a service;
 * 3. returning an HTTP response.
 *
 * Business logic belongs in the service layer.
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    private final HealthService healthService;

    /**
     * Constructor injection makes dependencies explicit
     * and keeps the class easy to test.
     */
    public HealthController(
            HealthService healthService
    ) {

        this.healthService =
                healthService;
    }

    /**
     * GET /api/health
     */
    @GetMapping("/health")
    public ResponseEntity<ApiResponse<Map<String, Object>>> health() {

        Map<String, Object> health =
                healthService.getHealthStatus();

        return ResponseEntity.ok(
                ApiResponse.success(
                        "EventPulse backend is running.",
                        health
                )
        );
    }
}
