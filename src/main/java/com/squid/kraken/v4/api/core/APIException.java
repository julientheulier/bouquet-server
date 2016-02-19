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

import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("serial")
public class APIException extends RuntimeException {

	static public enum ApiError {
		DUPLICATE_LOGIN, DUPLICATE_EMAIL, PASSWORD_REQUIRED, PASSWORD_INVALID_SIZE, PASSWORD_INVALID_CHAR, PASSWORD_INVALID_RULES, COMPUTING_IN_PROGRESS, COMPUTING_FAILED, NOT_IN_CACHE
	};

	private final boolean noError;

	private final ApiError apiError;

	public APIException(boolean noError) {
		this(null, null, noError, null);
	}

	public APIException(String message, Throwable cause, boolean noError) {
		this(message, cause, noError, null);
	}

	public APIException(String message, Throwable cause, boolean noError,
			ApiError apiError) {
		super(message, cause);
		this.noError = noError;
		this.apiError = apiError;
	}

	public APIException(String message, boolean noError) {
		this(message, null, noError, null);
	}

	public APIException(String message, boolean noError, ApiError apiError) {
		this(message, null, noError, apiError);
	}

	public APIException(Throwable cause, boolean noError) {
		this(null, cause, noError, null);
	}

	/**
	 * HTTP response code.
	 */
	public Integer getCode() {
		if (isNoError()) {
			// force 200 status (This to workaround a bug in jquery's jsonp
			// implementation)
			return 200;
		} else {
			return getErrorCode();
		}
	}

	protected Integer getErrorCode() {
		return 500;
	}

	/**
	 * Type (Exception class name) of the error.
	 */
	public String getType() {
		return this.getClass().getSimpleName();
	}

	public String getError() {
		return this.getMessage();
	}

	/**
	 * API Error code.
	 */
	public ApiError getApiError() {
		return apiError;
	}

	/**
	 * If set the returned REST Exception should use an HTTP 200 OK response
	 * code instead of a 401 in case of invalid token. This to workaround a bug
	 * in jquery's jsonp implementation.
	 */
	@JsonIgnore
	public boolean isNoError() {
		return noError;
	}

	@JsonIgnore
	@Override
	public String getMessage() {
		return super.getMessage();
	}

	@JsonIgnore
	@Override
	public String getLocalizedMessage() {
		return super.getLocalizedMessage();
	}

	@JsonIgnore
	@Override
	public Throwable getCause() {
		return super.getCause();
	}

	@JsonIgnore
	@Override
	public StackTraceElement[] getStackTrace() {
		return super.getStackTrace();
	}
	
	@Override
	public String toString() {
		// override to hide the className - don't want to have this to surface to end-user
        String s = getClass().getName();
        String message = getLocalizedMessage();
        return (message != null) ? message : s;
	}

}
