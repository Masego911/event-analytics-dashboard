package com.eventpulse.dto;

/**
 * Standard response object for simple EventPulse API messages.
 *
 * Using DTOs prevents controllers from returning arbitrary
 * structures and gives the API a predictable contract.
 */
public record ApiResponse<T>(
        boolean success,
        String message,
        T data
) {

    public static <T> ApiResponse<T> success(
            String message,
            T data
    ) {

        return new ApiResponse<>(
                true,
                message,
                data
        );
    }
}
