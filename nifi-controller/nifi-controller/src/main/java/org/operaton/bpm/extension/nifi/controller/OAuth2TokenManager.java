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

    public synchronized String getAccessToken() throws OperatonException {
        if (cachedAccessToken == null || isExpiring()) {
            refreshToken();
        }
        return cachedAccessToken;
    }

    public synchronized void invalidate() {
        this.cachedAccessToken = null;
        this.expirationTime = null;
    }

    public boolean isExpiring() {
        if (expirationTime == null) {
            return true;
        }
        return clock.get().isAfter(expirationTime.minus(refreshWindow));
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
            throw new OperatonException("OAuth2 token request failed with HTTP " + status + ": "
                    + (responseBody != null && !responseBody.isBlank() ? responseBody : "(empty response)"), status);
        }

        try {
            JsonNode json = mapper.readTree(responseBody);
            JsonNode tokenNode = json.get("access_token");
            if (tokenNode == null || tokenNode.asText().isBlank()) {
                throw new OperatonException("OAuth2 token response missing access_token", status);
            }
            this.cachedAccessToken = tokenNode.asText().trim();
            long expiresIn = json.hasNonNull("expires_in")
                    ? json.get("expires_in").asLong(DEFAULT_EXPIRES_IN_SECONDS)
                    : DEFAULT_EXPIRES_IN_SECONDS;
            if (expiresIn <= 0) {
                expiresIn = DEFAULT_EXPIRES_IN_SECONDS;
            }
            this.expirationTime = clock.get().plusSeconds(expiresIn);
        } catch (JsonProcessingException e) {
            throw new OperatonException("Failed to parse OAuth2 token response: " + e.getMessage(), status, e);
        }
    }
}
