package org.operaton.bpm.extension.nifi.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Manages OAuth2 access tokens acquired via client_credentials grant.
 * Fetches tokens on demand, caches them until near expiration, and refreshes them transparently.
 */
public class OAuth2TokenManager {

    public static final Duration DEFAULT_REFRESH_WINDOW = Duration.ofSeconds(30);
    public static final long DEFAULT_EXPIRES_IN_SECONDS = 300L;
    public static final long MAX_EXPIRES_IN_SECONDS = 31_536_000L; // 365 days
    private static final int MAX_ERROR_BODY_LENGTH = 500;

    private final HttpClient httpClient;
    private final String tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final Duration requestTimeout;
    private final ObjectMapper mapper;
    private final Supplier<Instant> clock;
    private final Duration refreshWindow;

    private volatile String cachedAccessToken;
    private volatile Instant expirationTime;
    private volatile long cachedExpiresIn;

    public OAuth2TokenManager(HttpClient httpClient, String tokenEndpoint, String clientId,
                              String clientSecret, String scope, Duration requestTimeout,
                              ObjectMapper mapper) {
        this(httpClient, tokenEndpoint, clientId, clientSecret, scope, requestTimeout, mapper,
                Instant::now, DEFAULT_REFRESH_WINDOW);
    }

    public OAuth2TokenManager(HttpClient httpClient, String tokenEndpoint, String clientId,
                              String clientSecret, String scope, Duration requestTimeout,
                              ObjectMapper mapper, Supplier<Instant> clock, Duration refreshWindow) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
        this.tokenEndpoint = Objects.requireNonNull(tokenEndpoint, "tokenEndpoint must not be null");
        this.clientId = Objects.requireNonNull(clientId, "clientId must not be null");
        this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret must not be null");
        this.scope = scope;
        this.requestTimeout = requestTimeout != null ? requestTimeout : Duration.ofSeconds(30);
        this.mapper = mapper != null ? mapper : new ObjectMapper();
        this.clock = clock != null ? clock : Instant::now;
        this.refreshWindow = refreshWindow != null ? refreshWindow : DEFAULT_REFRESH_WINDOW;
    }

    /**
     * Returns the cached access token or acquires a fresh token if expired or near expiration.
     * Uses double-checked locking to avoid synchronization overhead on cache hits.
     */
    public String getAccessToken() throws OperatonException {
        String token = this.cachedAccessToken;
        if (token != null && !isExpiring()) {
            return token;
        }
        synchronized (this) {
            if (this.cachedAccessToken == null || isExpiring()) {
                refreshToken();
            }
            return this.cachedAccessToken;
        }
    }

    /**
     * Unconditionally invalidates the cached token.
     */
    public synchronized void invalidate() {
        this.cachedAccessToken = null;
        this.expirationTime = null;
        this.cachedExpiresIn = 0;
    }

    /**
     * Conditionally invalidates the cached token only if it matches the token that failed.
     * Prevents concurrent requests from invalidating a freshly refreshed token.
     */
    public synchronized void invalidate(String failedToken) {
        if (failedToken == null || failedToken.equals(this.cachedAccessToken)) {
            invalidate();
        }
    }

    /**
     * Checks if the cached token is expired or within the refresh window.
     * When expiresIn is shorter than refreshWindow, the effective refresh window
     * adapts to at most 50% of expiresIn so short-lived tokens remain cached.
     */
    public boolean isExpiring() {
        Instant exp = expirationTime;
        if (exp == null) {
            return true;
        }
        long safeWindowSeconds = Math.min(refreshWindow.toSeconds(), Math.max(0, cachedExpiresIn / 2));
        return clock.get().isAfter(exp.minusSeconds(safeWindowSeconds));
    }

    public Instant getExpirationTime() {
        return expirationTime;
    }

    private void refreshToken() throws OperatonException {
        StringBuilder form = new StringBuilder();
        form.append("grant_type=client_credentials");
        form.append("&client_id=").append(URLEncoder.encode(clientId, StandardCharsets.UTF_8));
        form.append("&client_secret=").append(URLEncoder.encode(clientSecret, StandardCharsets.UTF_8));
        if (scope != null && !scope.isBlank()) {
            form.append("&scope=").append(URLEncoder.encode(scope.trim(), StandardCharsets.UTF_8));
        }

        HttpRequest request = HttpRequest.newBuilder(URI.create(tokenEndpoint))
                .timeout(requestTimeout)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form.toString(), StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new OperatonException("Failed to acquire OAuth2 access token: " + e.getMessage(), 0, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OperatonException("OAuth2 token request was interrupted", 0, e);
        }

        int status = response.statusCode();
        String responseBody = response.body();
        if (status < 200 || status >= 300) {
            String errorMsg = responseBody != null && !responseBody.isBlank()
                    ? (responseBody.length() <= MAX_ERROR_BODY_LENGTH
                            ? responseBody : responseBody.substring(0, MAX_ERROR_BODY_LENGTH) + "...")
                    : "(empty response)";
            throw new OperatonException("OAuth2 token request failed with HTTP " + status + ": " + errorMsg, status);
        }

        try {
            JsonNode json = mapper.readTree(responseBody);
            JsonNode tokenNode = json.get("access_token");
            if (tokenNode == null || tokenNode.asText().isBlank()) {
                throw new OperatonException("OAuth2 token response missing access_token", status);
            }

            JsonNode typeNode = json.get("token_type");
            if (typeNode != null && !typeNode.isNull() && !typeNode.asText().isBlank()) {
                if (!"bearer".equalsIgnoreCase(typeNode.asText().trim())) {
                    throw new OperatonException("Unsupported OAuth2 token_type: " + typeNode.asText(), status);
                }
            }

            this.cachedAccessToken = tokenNode.asText().trim();
            long expiresIn = json.hasNonNull("expires_in")
                    ? json.get("expires_in").asLong(DEFAULT_EXPIRES_IN_SECONDS)
                    : DEFAULT_EXPIRES_IN_SECONDS;
            if (expiresIn <= 0) {
                expiresIn = DEFAULT_EXPIRES_IN_SECONDS;
            }
            expiresIn = Math.min(expiresIn, MAX_EXPIRES_IN_SECONDS);
            this.cachedExpiresIn = expiresIn;
            this.expirationTime = clock.get().plusSeconds(expiresIn);
        } catch (JsonProcessingException e) {
            throw new OperatonException("Failed to parse OAuth2 token response: " + e.getMessage(), status, e);
        }
    }
}
