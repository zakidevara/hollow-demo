package com.devara.hollow.config;

import org.springframework.context.annotation.Configuration;

/**
 * Hollow UI Configuration
 * 
 * Note: The Netflix Hollow Explorer UI (hollow-explorer-ui) uses javax.servlet,
 * which is not compatible with Spring Boot 4.0+ (Jakarta EE 10).
 * 
 * For now, this application provides REST API endpoints to interact with Hollow data.
 * A custom web UI can be built using the REST API endpoints provided by HollowController.
 * 
 * Available endpoints:
 * - POST /api/hollow/publish - Publish data to Hollow
 * - POST /api/hollow/refresh - Refresh the consumer
 * - GET /api/hollow/status - Get current status and schema info
 * - GET /api/hollow/data - Browse data in the Hollow store
 */
@Configuration
public class HollowUIConfiguration {
    // UI configuration is disabled due to javax/jakarta servlet incompatibility
    // Use REST API endpoints instead
}
