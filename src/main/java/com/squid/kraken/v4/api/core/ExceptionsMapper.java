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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.cxf.rs.security.cors.CorsHeaderConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HttpHeaders;

@Provider
public class ExceptionsMapper implements ExceptionMapper<Throwable> {

	private static final Logger logger = LoggerFactory
			.getLogger(ExceptionsMapper.class);

	public Response toResponse(Throwable ex) {
		APIException apiException;
		if (ex instanceof WebApplicationException) {
			WebApplicationException wex = ((WebApplicationException) ex);
			if (!(wex.getCause() instanceof APIException)) {
				logger.warn("WebApplicationException", ex);
				return wex.getResponse();
			} else {
				apiException = (APIException) wex.getCause();
			}
		}

		if (ex instanceof APIException) {
			apiException = ((APIException) ex);
			if (apiException.getCode() == 500) {
				logger.warn("API Exception", ex);
			}
		} else {
			apiException = new APIException(ex.getMessage(), ex.getCause(),
					false);
			logger.warn("System error", ex);
		}

		ResponseBuilder builder = Response.status(apiException.getCode())
				.entity(apiException).type(MediaType.APPLICATION_JSON);

		// make sure CORS is handled
		builder.header(CorsHeaderConstants.HEADER_AC_ALLOW_CREDENTIALS, "false");
		builder.header(CorsHeaderConstants.HEADER_AC_ALLOW_ORIGIN, "*");

		// check if retry-after header should be honored
		if (apiException instanceof ComputingInProgressAPIException) {
			Integer retryAfter = ((ComputingInProgressAPIException) apiException)
					.getRetryAfter();
			if (retryAfter != null) {
				builder.header(HttpHeaders.RETRY_AFTER, retryAfter);
			}
		}

		return builder.build();
	}
}
