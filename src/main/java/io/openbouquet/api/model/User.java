package io.openbouquet.api.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.openbouquet.api.impl.UserImpl;

/**
 * Class that holds the information of a user's profile in Auth0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(as = UserImpl.class)
public interface User extends Serializable {
	
	public String getId() ;

	public String getName();

	public String getNickname();

	public String getEmail();

	public String getPictureURL();

	public String getGivenName();

	public String getFamilyName();
	
}