package com.eventpulse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main entry point for the EventPulse Spring Boot backend.
 *
 * Spring Boot begins component scanning from this package
 * and automatically discovers controllers, services,
 * repositories and configuration classes below it.
 */
@SpringBootApplication
public class EventPulseApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventPulseApplication.class, args);
    }
}
