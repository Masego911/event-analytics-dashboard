package com.eventpulse.controller;

import com.eventpulse.dto.dashboard.DashboardResponse;
import com.eventpulse.service.DashboardService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for the EventPulse audience dashboard.
 *
 * Notice that the controller contains no CSV parsing,
 * categorisation or audience calculations.
 *
 * Those responsibilities belong to DashboardService.
 */
@RestController
@RequestMapping("/api")
public class DashboardController {

    private final DashboardService dashboardService;

    public DashboardController(
            DashboardService dashboardService
    ) {

        this.dashboardService =
                dashboardService;
    }


    /**
     * Existing React contract:
     *
     * GET /api/dashboard
     */
    @GetMapping("/dashboard")
    public ResponseEntity<DashboardResponse> getDashboard() {

        return ResponseEntity.ok(
                dashboardService.getDashboard()
        );
    }
}
