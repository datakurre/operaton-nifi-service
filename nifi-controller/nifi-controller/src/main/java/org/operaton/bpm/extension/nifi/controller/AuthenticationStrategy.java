package org.operaton.bpm.extension.nifi.controller;

import org.apache.nifi.components.DescribedValue;

/**
 * How requests to the Operaton REST API are authenticated.
 */
public enum AuthenticationStrategy implements DescribedValue {

    NONE("None", "No authentication header is added to requests"),
    BASIC("HTTP Basic", "HTTP Basic authentication using username and password"),
    BEARER("Bearer Token", "Static Bearer token authentication using an Authorization header"),
    OAUTH2_PROVIDER("OAuth2 Access Token Provider", "OAuth2 access tokens managed by a controller service"),
    OAUTH2_CLIENT_CREDENTIALS("OAuth2 Client Credentials", "OAuth2 service account authentication via client_credentials grant");

    private final String displayName;
    private final String description;

    AuthenticationStrategy(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
