/*******************************************************************************
 * Copyright © Squid Solutions, 2016
 *
 * This file is part of Open Bouquet software.
 *  
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation (version 3 of the License).
 *
 * There is a special FOSS exception to the terms and conditions of the 
 * licenses as they are applied to this program. See LICENSE.txt in
 * the directory of this program distribution.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Squid Solutions also offers commercial licenses with additional warranties,
 * professional functionalities or services. If you purchase a commercial
 * license, then it supersedes and replaces any other agreement between
 * you and Squid Solutions (above licenses and LICENSE.txt included).
 * See http://www.squidsolutions.com/EnterpriseBouquet/
 *******************************************************************************/
package com.squid.kraken.v4.api.core;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
public class InvalidTokenAPIException extends APIException {

	private static final String ADMIN_CONSOLE = "admin_console";

	private final String loginURL;

	private final URI redirectURL;

	private final String clientId;

	public InvalidTokenAPIException(boolean noError, String loginURL) {
		this(null, loginURL, null, ADMIN_CONSOLE, noError);
	}

	public InvalidTokenAPIException(String message, boolean noError, String loginURL) {
		this(message, loginURL, null, ADMIN_CONSOLE, noError);
	}

	public InvalidTokenAPIException(String message, URI redirectURL, String clientId, boolean noError, String loginURL) {
		this(message, loginURL, redirectURL, clientId, noError);
	}

	public InvalidTokenAPIException(String message, Throwable cause, boolean noError, String loginURL) {
		this(message, loginURL, null, ADMIN_CONSOLE, noError);
	}

	public InvalidTokenAPIException(Throwable cause, boolean noError, String loginURL) {
		this(cause, null, loginURL, null, ADMIN_CONSOLE, noError);
	}
	
	public InvalidTokenAPIException(String message, String loginURL, URI redirectURL, String clientId,
			boolean noError) {
		super(message, noError);
		this.redirectURL = redirectURL;
		this.loginURL = loginURL;
		this.clientId = clientId;
	}

	public InvalidTokenAPIException(Throwable cause, String message, String loginURL, URI redirectURL, String clientId,
			boolean noError) {
		super(cause, noError);
		this.redirectURL = redirectURL;
		this.loginURL = loginURL;
		this.clientId = clientId;
	}

	@Override
	protected Integer getErrorCode() {
		return 401;
	}

	@JsonProperty
	public String getLoginURL() {
		return loginURL;
	}

	@JsonProperty
	public String getSelfLoginURL() {
		return redirectURL != null
				? getLoginURL() + "?client_id=" + clientId + "&redirect_uri=" + redirectURL.toASCIIString() : null;
	}
}
