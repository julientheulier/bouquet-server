package io.openbouquet.api.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that holds the information from a Identity Provider like Facebook or Twitter.
 */
@SuppressWarnings("serial")
public class UserIdentity implements Serializable {

    private String id;

    private String connection;

    private String provider;

    private boolean social;

    private String accessToken;

    private String accessTokenSecret;

    private Map<String, Object> profileInfo;

    public UserIdentity(String id, String connection, String provider, boolean social,
                        String accessToken, String accessTokenSecret, Map<String, Object> profileInfo) {
        this.id = id;
        this.connection = connection;
        this.provider = provider;
        this.social = social;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.profileInfo = profileInfo;
    }

    public String getId() {
        return id;
    }

    public String getConnection() {
        return connection;
    }

    public String getProvider() {
        return provider;
    }

    public boolean isSocial() {
        return social;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getAccessTokenSecret() {
        return accessTokenSecret;
    }

    public Map<String, Object> getProfileInfo() {
        return profileInfo != null ? new HashMap<>(profileInfo) : Collections.<String, Object>emptyMap();
    }
}