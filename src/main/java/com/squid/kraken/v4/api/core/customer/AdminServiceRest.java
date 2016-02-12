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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.EmailHelperImpl;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.CustomerInfo;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;

@Path("/admin")
@Api(value = "All", authorizations = { @Authorization(value = "kraken_auth", type = "oauth2") })
@Produces({ MediaType.APPLICATION_JSON })
/**
 * Admin Service holds the private API (not to be exposed to public access).
 * @author obalbous
 *
 */
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
	
	private AuthServiceImpl authService = AuthServiceImpl
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
		AppContext ctx = delegate.accessRequest(customerName, email, login, password, locale,
				reqIP, linkURL, defaultClientURL, EmailHelperImpl.getInstance());
		return delegate.readCustomerInfo(ctx);
	}
	
	/**
	 * Start the reset-password process :<br>
	 * Create a new 'reset_pwd' {@link AccessToken} for the user having the
	 * passed email address.<br>
	 * Send it by mail to the passed email address.<br>
	 * The email will contain a link built by replacing <tt>{access_token}</tt>
	 * by the token value in the provided link url which be checked for
	 * validity.
	 * 
	 * @param customerId
	 * @param clientId
	 * @param email
	 *            the email of the user account requesting a password reset.
	 * @param lang
	 *            the language used to build the email content or null for
	 *            default.
	 * @param linkURL
	 *            the link url base used to build the link enclosed in the email
	 *            (ie.
	 *            <tt>http://api.squisolutions.com/release/api/reset_email?access_token={access_token}</tt>
	 *            ). The url must match the {@link Client} authorized urls.
	 * @return an "ok" message.
	 */
	@Path("/reset-user-pwd")
	@GET
	@ApiOperation(value = "Start the reset-password process. Create a new 'reset_pwd' AccessToken for the user having the passed email address.")
	public String resetUserPassword(
			@Context HttpServletRequest request,
			@ApiParam(required = true) @QueryParam("customerId") String customerId,
			@ApiParam(required = true) @QueryParam("clientId") String clientId,
			@ApiParam(required = true, value = "the email of the user account requesting a password reset") @QueryParam("email") String email,
			@QueryParam("lang") String lang,
			@ApiParam(required = true, value = "the link url base used to build the link enclosed in the email"
					+ " (ie. http://api.squisolutions.com/release/api/reset_email?access_token={access_token})."
					+ " The url must match the Client authorized urls") @QueryParam("link_url") String linkURL) {
		String content = "Follow this link to reset your password : \n"
				+ "${resetLink}";
		content += "\n(this link will be valid for ${validity} hours)";
		String subject = "Password reset procedure";
		AppContext ctx = ServiceUtils.getInstance().getAnonymousUserContext(
				request, customerId, clientId);
		authService.resetUserPassword(ctx, EmailHelperImpl.getInstance(),
				clientId, email, lang, linkURL, content, subject);
		return "{ \"message\" : \"Reset password token sent, please check your emails.\" }";
	}

}
