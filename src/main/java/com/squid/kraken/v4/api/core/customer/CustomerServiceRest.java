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
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.squid.core.api.CoreVersion;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.EmailHelperImpl;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.bookmark.BookmarkFolderServiceRest;
import com.squid.kraken.v4.api.core.client.ClientServiceRest;
import com.squid.kraken.v4.api.core.connection.ConnectionServiceRest;
import com.squid.kraken.v4.api.core.internalAnalysisJob.InternalAnalysisJobServiceRest;
import com.squid.kraken.v4.api.core.project.ProjectServiceRest;
import com.squid.kraken.v4.api.core.user.UserServiceRest;
import com.squid.kraken.v4.api.core.usergroup.UserGroupServiceRest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.AuthCode;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.CustomerInfo;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.Authorization;
import com.wordnik.swagger.annotations.AuthorizationScope;

@Path("/rs")
@Api(value = "All", authorizations = { @Authorization(value = "kraken_auth", type = "oauth2", scopes = { @AuthorizationScope(scope = "access", description = "Access")}) })
@Produces({ MediaType.APPLICATION_JSON })
public class CustomerServiceRest {

	static private CustomerServiceRest instance;

	static public CustomerServiceRest getInstance() {
		if (instance == null) {
			instance = new CustomerServiceRest();
		}
		return instance;
	}

	static public final String PARAM_REFRESH = "refresh";

	static public final String PARAM_DEEP_READ = "deepread";

	static public final String PARAM_OPTION = "option";

	private CustomerServiceBaseImpl delegate = CustomerServiceBaseImpl
			.getInstance();

	private AuthServiceImpl authService = AuthServiceImpl.getInstance();

	// CustomerServiceRest is a singleton
	public CustomerServiceRest() {
		super();
	}

	@Path("/connections")
	@ApiOperation(value = "Manage database connection")
	public ConnectionServiceRest getConnectionService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new ConnectionServiceRest(userContext);
	}

	@Path("/projects")
	@ApiOperation(value = "Gets the Projects")
	public ProjectServiceRest getProjectService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new ProjectServiceRest(userContext);
	}

	@Path("/users")
	@ApiOperation(value = "Gets Users")
	public UserServiceRest getUserService(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new UserServiceRest(userContext);
	}

	@Path("/usergroups")
	@ApiOperation(value = "Gets UserGroups")
	public UserGroupServiceRest getUserGroupServiceDeprecated(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new UserGroupServiceRest(userContext);
	}
	
	@Path("/userGroups")
	@ApiOperation(value = "Gets UserGroups")
	public UserGroupServiceRest getUserGroupService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new UserGroupServiceRest(userContext);
	}

	@Path("/clients")
	@ApiOperation(value = "Gets Clients")
	public ClientServiceRest getClientService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new ClientServiceRest(userContext);
	}

	@Path("/states")
	@ApiOperation(value = "Gets states")
	public StateServiceRest getStateService(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new StateServiceRest(userContext);
	}

	@Path("/shortcuts")
	@ApiOperation(value = "Gets shortcuts")
	public ShortcutServiceRest getShortcutService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new ShortcutServiceRest(userContext);
	}
	
	@Path("/bookmarkfolders")
	@ApiOperation(value = "Gets bookmarkFolders")
	public BookmarkFolderServiceRest getBookmarkFolderService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new BookmarkFolderServiceRest(userContext);
	}

	
	@Path("/queries")
	@ApiOperation(value = "Gets ongoing queries")
	public QueriesServiceRest getQueriesService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new QueriesServiceRest(userContext);
	}

	@Path("/internalanalysisjobs")
	@ApiOperation(value = "Gets internal analysis jobs")
	public InternalAnalysisJobServiceRest getInternalAnalysisService(
			@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return new InternalAnalysisJobServiceRest(userContext);
	}

	/**
	 * Token Validation.<br>
	 * Tokens received via OAuth 2.0 Client-Side flow must be explicitly
	 * validated.<br>
	 * Failure to verify tokens acquired this way makes your application more
	 * vulnerable to the 'confused deputy problem'.
	 */
	@Path("/tokeninfo")
	@GET
	@ApiOperation(value = "Token Validation. Tokens received via OAuth 2.0 Client-Side flow must be explicitly validated.")
	public AccessToken getTokenInfo(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return userContext.getToken();
	}
	
	/**
	 * Delete Token.
	 */
	@Path("/token/{tokenId}")
	@DELETE
	@ApiOperation(value = "Deletes a token")
	public boolean deleteToken(@Context HttpServletRequest request, @PathParam("tokenId") String tokenId) {
		AppContext userContext = getAnonymousUserContext(request, null, null);
		DAOFactory.getDAOFactory().getDAO(AccessToken.class).delete(userContext, new AccessTokenPK(tokenId));
		return true;
	}

	/**
	 * Retrieve an {@link AccessToken} either given an Auth Code or a Refresh Token or a JWT.<br>
	 */
	@Path("/token")
	@POST
	@ApiOperation(value = "Retrieve an AccessToken given an AuthCode or a Refresh Token or a JWT")
	public AccessToken authToken(
			@Context HttpServletRequest request,
			@ApiParam(required = true) @FormParam("grant_type") String grantType,
			@ApiParam @FormParam("code") String code,
			@ApiParam @FormParam("refresh_token") String refreshToken,
			@ApiParam(required = true) @FormParam("client_id") String clientId,
			@ApiParam @FormParam("client_secret") String clientSecret,
			@ApiParam(required = true) @FormParam("redirect_uri") String redirectUri,
			@ApiParam @FormParam("assertion") String assertion) {
		AppContext userContext = getAnonymousUserContext(request, null,
				clientId);
		// process
		ClientPK clientPk = new ClientPK(userContext.getCustomerId(), clientId);
		if (code != null) {
			// auth code
			return authService.getTokenFromAuthCode(userContext, clientPk,
					redirectUri, code);
		} else if (refreshToken != null) {
			// refresh token
			return authService.getTokenFromRefreshToken(userContext, clientPk, clientSecret,
					redirectUri, refreshToken);
		} else if (assertion != null) {
			// JWT
			return authService.getTokenFromJWT(userContext, assertion);
		} else {
			throw new APIException(
					"Invalid request. Either 'code' or 'refresh_token' or 'assertion' should be set",
					userContext.isNoError());
		}
	}

	/**
	 * OAuth 2 "Client-Side Flow" Authentication.<br>
	 * Generates a new {@link AccessToken} for a given User.<br>
	 * Redirect URL check will be performed to make sure its domain matches a
	 * least one of the Client URLs (bypassed if no client URLS defined).<br>
	 * If customerId is null, a lookup will be performed to find all matching
	 * login/pwd accounts.
	 * 
	 * @param customerId
	 *            or null
	 * @param clientId
	 * @param redirectUri
	 * @param login
	 *            user login
	 * @param password
	 *            user password
	 * 
	 * @return a token having default validity
	 * @throws DuplicateUserException
	 *             if customerId is null and more that one user account matches
	 *             login/pwd.
	 */
	@Path("/auth-token")
	@POST
	@ApiOperation(value = "OAuth 2 'Client-Side Flow' Authentication. Generates a new AccessToken for a given User.")
	public AccessToken authToken(
			@Context HttpServletRequest request,
			@ApiParam(required = true) @FormParam("customerId") String customerId,
			@ApiParam(required = true) @FormParam("client_id") String clientId,
			@ApiParam(required = true) @FormParam("redirect_uri") String redirectUri,
			@ApiParam(required = true) @FormParam("login") String login,
			@ApiParam(required = true) @FormParam("password") String password)
			throws DuplicateUserException {
		AppContext userContext = getAnonymousUserContext(request, customerId,
				clientId);
		// process
		ClientPK clientPk = new ClientPK(customerId, clientId);
		return authService.authAndReturnToken(userContext, clientPk,
				redirectUri, login, password);
	}

	/**
	 * OAuth 2 "Server-Side Flow" Authentication.<br>
	 * Generates a new {@link AuthCode} for a given User.<br>
	 * Redirect URL check will be performed to make sure its domain matches a
	 * least one of the Client URLs (bypassed if no client URLS defined).<br>
	 * If customerId is null, a lookup will be performed to find all matching
	 * login/pwd accounts.
	 * 
	 * @param customerId
	 *            or null
	 * @param clientId
	 * @param redirectUrl
	 * @param login
	 *            user login
	 * @param password
	 *            user password
	 * @param accessType
	 *            'online' or 'offline'. Default is 'online'. If 'offline' first
	 *            authToken resquest will return an refresh-token.
	 * 
	 * @return a auth code
	 * @throws DuplicateUserException
	 *             if customerId is null and more that one user account matches
	 *             login/pwd.
	 */
	@Path("/auth-code")
	@POST
	@ApiOperation(value = "OAuth 2 'Server-Side Flow' Authentication. Generates a new AuthCode for a given User.")
	public AuthCode authCode(
			@Context HttpServletRequest request,
			@ApiParam(required = true) @FormParam("customerId") String customerId,
			@ApiParam(required = true) @FormParam("client_id") String clientId,
			@ApiParam(required = true) @FormParam("redirect_uri") String redirectUrl,
			@ApiParam(required = true) @FormParam("login") String login,
			@ApiParam(required = true) @FormParam("password") String password,
			@ApiParam @FormParam("access_type") String accessType) {
		AppContext userContext = getAnonymousUserContext(request, customerId,
				clientId);
		// process
		ClientPK clientPk = new ClientPK(customerId, clientId);
		boolean generateRefreshToken = false;
		if ((accessType != null) && (accessType.equals("offline"))) {
			generateRefreshToken = true;
		}
		return authService.authAndReturnCode(userContext, clientPk,
				redirectUrl, login, password, generateRefreshToken);
	}

	/**
	 * Logout the current User (identified by the AccessToken) by invalidating
	 * all its {@link AccessToken}s.
	 */
	@Path("/logout")
	@GET
	@ApiOperation(value = "Logout the current User (identified by the AccessToken) by invalidating all its AccessTokens")
	public String logoutUser(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		authService.logoutUser(userContext);
		return "{ \"logout\" : \"true\" }";
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
			@ApiParam(required = true, value = "the link url base used to build the link enclosed in the email (ie. http://api.squisolutions.com/release/api/reset_email?access_token={access_token}). The url must match the Client authorized urls") @QueryParam("link_url") String linkURL) {
		String content = "Follow this link to reset your password : \n"
				+ "${resetLink}";
		content += "\n(this link will be valid for ${validity} hours)";
		String subject = "Password reset procedure";
		AppContext ctx = getAnonymousUserContext(request, customerId, clientId);
		authService.resetUserPassword(ctx, EmailHelperImpl.getInstance(),
				clientId, email, lang, linkURL, content, subject);
		return "{ \"message\" : \"Reset password token sent, please check your emails.\" }";
	}

	/**
	 * Part of the user creation process :<br>
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
	 *            the email of the user account.
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
	@Path("/set-user-pwd")
	@GET
	@ApiOperation(value = "Part of the user creation process. Create a new 'reset_pwd' AccessToken for the user having the passed email address")
	public String sendUserPasswordEmail(
			@Context HttpServletRequest request,
			@ApiParam(required = true) @QueryParam("customerId") String customerId,
			@ApiParam(required = true) @QueryParam("clientId") String clientId,
			@ApiParam(required = true, value = "the email of the user account") @QueryParam("email") String email,
			@QueryParam("lang") String lang,
			@ApiParam(required = true, value = "the link url base used to build the link enclosed in the email (ie. http://api.squisolutions.com/release/api/reset_email?access_token={access_token}). The url must match the Client authorized urls") @QueryParam("link_url") String linkURL) {

		String content = "Your account has just been created.\n"
				+ "Please follow the link "
				+ "${resetLink} to define your password";
		content += "\n(this link will be valid for ${validity} hours)";
		String subject = "New Account";

		AppContext ctx = getAnonymousUserContext(request, customerId, clientId);
		authService.resetUserPassword(ctx, EmailHelperImpl.getInstance(),
				clientId, email, lang, linkURL, content, subject);
		return "{ \"message\" : \"Set password token sent, please check your emails.\" }";
	}

	/**
	 * Get the current User (identified by the AccessToken).
	 */
	@Path("/user")
	@GET
	@ApiOperation(value = "Get the current User (identified by the AccessToken)")
	public User readContextUser(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return delegate.readContextUser(userContext);
	}

	/**
	 * Get the public Customer information based on the Token passed in.
	 * 
	 * @return CustomerInfo
	 */
	@Path("/")
	@GET
	@ApiOperation(value = "Get the public Customer information based on the Token passed in.")
	public CustomerInfo getCustomerInfo(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		CustomerInfo customer = delegate.readCustomerInfo(userContext);
		return customer;
	}

	/**
	 * Update Customer information based on the Token passed in.
	 * 
	 * @return CustomerInfo
	 */
	@Path("/")
	@PUT
	@ApiOperation(value = "Update Customer information based on the Token passed in.")
	public CustomerInfo updateCustomerInfo(@Context HttpServletRequest request,
			@ApiParam(required = true) CustomerInfo customerInfo) {
		AppContext userContext = getUserContext(request);
		CustomerInfo customer = delegate.updateCustomerInfo(userContext,
				customerInfo);
		return customer;
	}

	@Path("/access")
	@GET
	@ApiOperation(value = "Get the Customer's access rights")
	public Set<AccessRight> readAccessRights(@Context HttpServletRequest request) {
		AppContext userContext = getUserContext(request);
		return delegate.readAccessRights(userContext, new CustomerPK(
				userContext.getCustomerId()));
	}

	@Path("/access")
	@POST
	@ApiOperation(value = "Update the Customer's access rights")
	public Set<AccessRight> storeAccessRights(
			@Context HttpServletRequest request,
                        @ApiParam(required = true) Set<AccessRight> accessRights) {
		AppContext userContext = getUserContext(request);
		return delegate.storeAccessRights(userContext,
				userContext.getCustomerPk(), accessRights);
	}

	/**
	 * Retrieve the platform build version.
	 * 
	 * @return Json String.
	 */
	@Path("/status")
	@GET
	@ApiOperation(value = "Get the platform build version.", authorizations = {})
	public String status(@Context HttpServletRequest request) {
		// log the request
		ServiceUtils.getInstance().logAPIRequest(null, null, null, null,
				request);
		String res = "{ \"bouquet-server\" : "
				+ ServiceUtils.getInstance().getBuildVersionString();

		res += ",\"bouquet-plugins\" : [ ";
		boolean first = true;
		for (IVendorSupport plugin : VendorSupportRegistry.INSTANCE
				.listVendors()) {
			if (!first) {
				res += ",";
			} else {
				first = false;
			}
			res += "{\"" + plugin.getVendorId() + "\" : \""
					+ plugin.getVendorVersion() + "\"";
			res += ",\"vendorId\":\"" + plugin.getVendorId() + "\"";
			res += ",\"version\":\"" + plugin.getVendorVersion() + "\"";
			res += ",\"jdbcTemplate\":" + plugin.getJdbcUrlTemplate();
			res +=  "}";
		}
		res += "]";
		CoreVersion version = new CoreVersion();
		res += ", \"bouquet-core\" : \"" + version.getVendorVersion() + "\"";

		res += " }";
		return res;
	}

	// some utility methods

	/**
	 * Build an {@link AppContext} from an {@link HttpServletRequest} and log
	 * the request.
	 * 
	 * @return an {@link AppContext}
	 * @throws TokenExpiredException
	 *             if the token has expired.
	 */
	private AppContext getUserContext(HttpServletRequest request) {
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
	private AppContext getAnonymousUserContext(HttpServletRequest request,
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
