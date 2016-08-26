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
package com.squid.kraken.v4.api.core.customer;

import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;

import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

/**
 * Handle the UserContext based on the HTTP request
 * (code extracted from CustomerServiceRest to share with BB API)
 * @author sergefantino
 *
 */
public class CoreAuthenticatedServiceRest {

	static private final String PARAM_REFRESH = "refresh";

	static private final String PARAM_DEEP_READ = "deepread";

	static private final String PARAM_OPTION = "option";

	// some utility methods

	/**
	 * Build an {@link AppContext} from an {@link HttpServletRequest} and log
	 * the request.
	 * 
	 * @return an {@link AppContext}
	 * @throws TokenExpiredException
	 *             if the token has expired.
	 */
	protected AppContext getUserContext(HttpServletRequest request) {
		ServiceUtils sutils = ServiceUtils.getInstance();
		AccessToken token = null;
		AppContext ctx = null;
		try {
			// retrieve the token
			token = sutils.getToken(request);

			// retrieve the User
			AppContext root = ServiceUtils.getInstance().getRootUserContext(
					token.getCustomerId());
			User user = DAOFactory
					.getDAOFactory()
					.getDAO(User.class)
					.readNotNull(
							root,
							new UserPK(token.getCustomerId(), token.getUserId()));

			// build the context
			boolean dryRun = sutils.isDryRunEnabled(request);
			boolean noError = sutils.isNoErrorEnabled(request);
			String locale = sutils.getLocale(request);
			AppContext.Builder ctxb = new AppContext.Builder(token, user)
					.setDryRun(dryRun).setLocale(locale).setNoError(noError);
			if (request.getParameter(PARAM_REFRESH) != null) {
				// cache invalidation
				ctxb.setRefresh(true);
			}
			if (request.getParameter(PARAM_DEEP_READ) != null) {
				ctxb.setDeepRead(true);
			}
			String sessionId = request.getHeader(AppContext.HEADER_BOUQUET_SESSIONID);
			if (sessionId != null) {
				ctxb.setSessionId(sessionId);
			}
			String[] options = request.getParameterValues(PARAM_OPTION);
			if (options != null) {
				ctxb.setOptions(Arrays.asList(options));
			}
			ctx = ctxb.build();
			return ctx;
		} finally {
			// log the request
			sutils.logAPIRequest(ctx, request);
		}
	}

	/**
	 * Build an {@link AppContext} from an {@link HttpServletRequest}
	 */
	protected AppContext getAnonymousUserContext(HttpServletRequest request,
			String customerId, String clientId) {
		ServiceUtils sutils = ServiceUtils.getInstance();
		AppContext ctx = null;
		try {
			boolean dryRun = sutils.isDryRunEnabled(request);
			boolean noError = sutils.isNoErrorEnabled(request);
			String locale = sutils.getLocale(request);
			AppContext.Builder ctxb = new AppContext.Builder(customerId,
					clientId).setDryRun(dryRun).setLocale(locale)
					.setNoError(noError);
			if (request.getParameter(PARAM_REFRESH) != null) {
				// perform cache invalidation
				ctxb.setRefresh(true);
			}
			ctx = ctxb.build();
			return ctx;
		} finally {
			// log the request
			sutils.logAPIRequest(ctx, request);
		}
	}
	
}
