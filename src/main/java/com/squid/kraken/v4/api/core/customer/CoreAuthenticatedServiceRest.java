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

import javax.servlet.http.HttpServletRequest;

import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * Handle the UserContext based on the HTTP request
 * (code extracted from CustomerServiceRest to share with BB API)
 * @author sergefantino
 *
 */
public class CoreAuthenticatedServiceRest {

	/**
	 * Build an {@link AppContext} from an {@link HttpServletRequest} and log
	 * the request.
	 * 
	 * @return an {@link AppContext}
	 * @throws TokenExpiredException
	 *             if the token has expired.
	 */
	protected AppContext getUserContext(HttpServletRequest request) {
		return ServiceUtils.getInstance().getUserContext(request);
	}

	/**
	 * Build an {@link AppContext} from an {@link HttpServletRequest}
	 */
	protected AppContext getAnonymousUserContext(HttpServletRequest request,
			String customerId, String clientId) {
		return ServiceUtils.getInstance().getAnonymousUserContext(request, customerId, clientId);
	}
	
	
}
