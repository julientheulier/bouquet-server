package io.openbouquet.api.impl;

import io.openbouquet.api.model.User;

/**
 * Class that holds the information of a user's profile in Auth0
 */
@SuppressWarnings("serial")
public class UserImpl implements User {
	private String id;
	private String name;
	private String nickname;
	private String pictureURL;

	private String email;
	private String givenName;
	private String familyName;

	public UserImpl() {
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getNickname() {
		return nickname;
	}

	public String getEmail() {
		return email;
	}

	public String getPictureURL() {
		return pictureURL;
	}

	public String getGivenName() {
		return givenName;
	}

	public String getFamilyName() {
		return familyName;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + "]";
	}

}