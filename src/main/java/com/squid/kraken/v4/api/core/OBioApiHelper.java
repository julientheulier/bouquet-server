package com.squid.kraken.v4.api.core;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.KrakenConfig;

import io.openbouquet.api.service.MembershipService;
import io.openbouquet.api.service.UserProfileService;

/**
 * OB io API Helper class
 * @author obalbous
 */
@SuppressWarnings("serial")
public class OBioApiHelper implements Serializable {

	final static Logger logger = LoggerFactory.getLogger(OBioApiHelper.class);

	private static OBioApiHelper instance;

	public static OBioApiHelper getInstance() {
		if (instance == null) {
			instance = new OBioApiHelper();
			if (apiEndpoint == null) {
				OBioApiHelper.setApiEndpoint(KrakenConfig.getProperty("ob-io-api.endpoint"));
			}
		}
		return instance;
	}

	private static String apiEndpoint;

	public static void setApiEndpoint(String endpoint) {
		apiEndpoint = endpoint;
	}

	private MembershipService membershipService;
	private UserProfileService userProfileService;

	private OBioApiHelper() {
	}

	public MembershipService getMembershipService() {
		if (membershipService == null) {
			membershipService = new RestServiceProxy<MembershipService>().newInstance(MembershipService.class,
					apiEndpoint);
		}
		return membershipService;
	}

	public UserProfileService getUserProfileService() {
		if (userProfileService == null) {
			userProfileService = new RestServiceProxy<UserProfileService>().newInstance(UserProfileService.class,
					apiEndpoint);
		}
		return userProfileService;
	}

}