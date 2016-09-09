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
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.EmailHelperImpl;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.CustomerInfo;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.Api;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;

@Path("/admin")
@Api(value = "All", authorizations = { @Authorization(value = "kraken_auth", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class AdminServiceRest {

	static private AdminServiceRest instance;

	static public AdminServiceRest getInstance() {
		if (instance == null) {
			instance = new AdminServiceRest();
		}
		return instance;
	}

	private CustomerServiceBaseImpl delegate = CustomerServiceBaseImpl
			.getInstance();

	public AdminServiceRest() {
		super();
	}

	/**
	 * Request for access to the system. The process will be the following:
	 * <ul>
	 * <li>Create a new Customer with given name.</li>
	 * <li>Create a new User with Owner rights on Customer with given email.</li>
	 * <li>Create a new Client with domain "default.client.url" for this
	 * Customer (Client.urls - new field in the Client model).</li>
	 * <li>If an Email is provided, send a welcome mail including a link to the API Console.</li>
	 * </ul>
	 * 
	 * @param request
	 *            http request
	 * @param customerName
	 *            name of the new customer
	 * @param email
	 *            email of the new user
	 * @param login
	 *            login of the new user
	 * @param password
	 *            password of the new user
	 * @param locale
	 *            locale
	 * @param linkURL
	 *            the link url base used to build the link enclosed in the confimation email
	 *            (eg.
	 *            <tt>https://api.squidsolutions.com/release/admin/console/index.html?customerId={customerId}</tt>
	 *            )
	 * @return {@link CustomerInfo}
	 */
	@Path("/create-customer")
	@POST
	public CustomerInfo createCustomer(@Context HttpServletRequest request,
			@FormParam("customerName") String customerName,
			@FormParam("email") String email, 
			@FormParam("login") String login,
			@FormParam("password") String password,
			@FormParam("locale") String locale,
			@FormParam("linkURL") String linkURL) {
		// log the request
		ServiceUtils.getInstance().logAPIRequest(null, null, null, login,
				request);
		// perform the request
		String reqIP = ServiceUtils.getInstance().getRemoteIP(request);
		String defaultClientURL = KrakenConfig.getProperty(
				"default.client.url", true);
		AppContext ctx = delegate.accessRequest(AUTH_MODE.OAUTH, customerName, email, login, password, locale,
				reqIP, linkURL, defaultClientURL, EmailHelperImpl.getInstance());
		return delegate.readCustomerInfo(ctx);
	}

}
