package com.squid.kraken.v4.api.core;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openbouquet.api.service.MembershipService;
import io.openbouquet.api.service.UserProfileService;

@SuppressWarnings("serial")
public class OBioApiHelper implements Serializable {

	final static Logger logger = LoggerFactory.getLogger(OBioApiHelper.class);

	private static OBioApiHelper instance;

	public static OBioApiHelper getInstance() {
		if (instance == null) {
			instance = new OBioApiHelper();
		}
		return instance;
	}

	private static String authServerEndpoint;

	public static void setAuthServerEndpoint(String endpoint) {
		authServerEndpoint = endpoint;
	}

	private MembershipService membershipService;
	private UserProfileService userProfileService;

	private OBioApiHelper() {
	}

	public MembershipService getMembershipService() {
		if (membershipService == null) {
			membershipService = new RestServiceProxy<MembershipService>().newInstance(MembershipService.class,
					authServerEndpoint);
		}
		return membershipService;
	}

	public UserProfileService getUserProfileService() {
		if (userProfileService == null) {
			userProfileService = new RestServiceProxy<UserProfileService>().newInstance(UserProfileService.class,
					authServerEndpoint);
		}
		return userProfileService;
	}

}