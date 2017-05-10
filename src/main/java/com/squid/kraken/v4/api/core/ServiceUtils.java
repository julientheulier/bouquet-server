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

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.customer.AuthServiceImpl;
import com.squid.kraken.v4.api.core.customer.StateServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.TokenExpiredException;
import com.squid.kraken.v4.api.core.projectanalysisjob.AnalysisJobServiceBaseImpl;
import com.squid.kraken.v4.api.core.projectfacetjob.FacetJobServiceBaseImpl;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.core.analysis.engine.cache.MetaModelObserver;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStoreEventBus;
import com.squid.kraken.v4.persistence.dao.CustomerDAO;

public class ServiceUtils {

	private static final String NULL = "null";

	private static final String AUTHORIZATION = "Authorization";

	private static final Logger loggerAPI = LoggerFactory.getLogger("API");

	private static final String ISO8601 = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	private static final String ISO8601short = "yyyy-MM-dd";
	// next one is actually the format used by the Date.toString()...
	private static final String JavaDateToStringFormat = "EEE MMM dd HH:mm:ss zzz yyyy";

	private static final String SQUIDAPILOCALE = "squidapilocale";

	private static final String SQUIDAPITOKEN = "squidapitoken";

	public final static String TOKEN_PARAM = "access_token";
	
	public final static String BEARER_HEADER = "Bearer ";

	public final static String EMAIL_PARAM = "email";

	public final static String LOCALE_PARAM = "locale";

	public final static String NO_ERROR_PARAM = "no_error";

	public final static String DRY_RUN_PARAM = "dry_run";

	public final static String ID_VALIDATION_REGEX = "[a-zA-Z_\\-\\:0-9]{1,1023}";

	public final static Pattern ID_VALIDATION_PATTERN = Pattern
			.compile(ID_VALIDATION_REGEX);

	public final static String ENGLISH_LOCALE = "en_en";

	public final static String FRENCH_LOCALE = "fr_fr";

	private static final String PRIVATE_IP_PREFIX_AWS = "10.";

	private static final String PRIVATE_IP_PREFIX_SQUID = "192.168.";

	public static final String HEADER_XFF = "X-Forwarded-For";

	private static final String ALGO = "AES";

	private static final ServiceUtils instance = new ServiceUtils();

	private String buildVersionString;

	private Map<String, AppContext> rootUsers = new HashMap<String, AppContext>();

	public static ServiceUtils getInstance() {
		return instance;
	}
	
	private final long resetPasswordTokenExpirationPeriodMillis = 48 * 60 * 60 * 1000;

	private final long tokenExpirationPeriodMillis = 24 * 60 * 60 * 1000;
	
	private final long codeExpirationPeriodMillis = 10 * 60 * 1000;

	public boolean isValidId(String id) {
		boolean match = true;
		if (id == null) {
			match = false;
		} else {
			Matcher matcher = ID_VALIDATION_PATTERN.matcher(id);
			if (!matcher.matches()) {
				match = false;
			}
		}
		return match;
	}

	public Boolean checkforSuperUserRootUserContext() {
		for (AppContext ctx : rootUsers.values()){
			List<User> users = UserServiceBaseImpl.getInstance().readAll(ctx);
			for(User user : users){
				if (user.getGroups() == null) {
					return (false || user.isSuperUser());
				}else{
					return (user.getGroups().contains("superuser") || user.isSuperUser());
				}
			}
		}

		return false;
	}

	public AppContext getRootUserContext(String customerId) {
		AppContext root = rootUsers.get(customerId);
		if (root == null) {
			User rootUser = new User(new UserPK(customerId, "root"), "root",
					null);
			rootUser.setSuperUser(true);
			root = new AppContext.Builder(customerId, rootUser).build();
			rootUsers.put(customerId, root);
		}
		return root;
	}
	
	public AppContext getRootUserContext(AppContext ctx) {
		return getRootUserContext(ctx.getCustomerId());
	}

	/**
	 * Get the customer associated to this AppContext.
	 * 
	 * @param ctx
	 *            AppContext
	 * @return Customer
	 */
	public Customer readCustomer(AppContext ctx) {
		// use a root context
		AppContext root = getRootUserContext(ctx.getCustomerId());
		return DAOFactory.getDAOFactory().getDAO(Customer.class)
				.readNotNull(root, new CustomerPK(ctx.getCustomerId()));
	}

	/**
	 * Retrieve a {@link AccessToken}.
	 * 
	 * @param tokenId
	 * @return the AccessToken associated to this token or a super-user token if authMode is BYPASS or
	 *         <tt>null</null>d.
	 * @throws TokenExpiredException
	 *             if the token has expired.
	 */
	public AccessToken getToken(String tokenId) throws TokenExpiredException {
		AccessToken token = null;
		if ((tokenId != null) && (!tokenId.equals(NULL))){
			Optional<AccessToken> tokenOpt = DAOFactory.getDAOFactory()
					.getDAO(AccessToken.class)
					.read(null, new AccessTokenPK(tokenId));
			if (tokenOpt.isPresent()) {
				token = tokenOpt.get();
				long now = System.currentTimeMillis();
				long validity = token.getExpirationDateMillis();
				if ((validity - now) > 0) {
					return token;
				} else {
					throw new TokenExpiredException(tokenId);
				}
			}
		}
		if (token == null) {
			if (KrakenConfig.getAuthMode() == AUTH_MODE.BYPASS) {
				// look for a Customer that has a a bypass auth mode
				Customer singleCustomer = null;
				AppContext ctx = new AppContext.Builder().build();
				List<Customer> all = ((CustomerDAO) DAOFactory.getDAOFactory()
						.getDAO(Customer.class)).findAll(ctx);
				for (Customer customer : all) {
					if (customer.getAuthMode() == AUTH_MODE.BYPASS) {
						singleCustomer = customer;
					}
				}
				if (singleCustomer != null) {
					// generate a new token
					token = createSuperUserToken(singleCustomer);
				}
			}
		}
		return token;
	}

	/**
	 * Create a special super-user token
	 * @param customer
	 * @return
	 */
	private AccessToken createSuperUserToken(Customer customer) {
		// get the customer owner user
		String userId = null;
		for (AccessRight r : customer.getAccessRights()) {
			userId  = r.getUserId();
			if ((r.getRole() == Role.OWNER) && (userId != null)) {
				break; 
			}
		}
		return this.createToken(customer.getOid(), null, userId,
				System.currentTimeMillis(), ServiceUtils
				.getInstance().getTokenExpirationPeriodMillis(), AccessToken.Type.NORMAL,
				null);
	}
	
	/**
	 * Create a new Token.
	 * 
	 * @param clientPk
	 * @param userId
	 * @param creationTimestamp
	 *            custom creation date or current date if <tt>null</tt>
	 * @param validityMillis
	 *            the token validity in milliseconds.
	 * @param token
	 *            type (or null)
	 * @return an AccessToken
	 */
	public AccessToken createToken(String customerId, ClientPK clientPk,
			String userId, Long creationTimestamp, Long validityMillis,
			AccessToken.Type type, String refreshTokenId) {
		AppContext rootContext = ServiceUtils.getInstance().getRootUserContext(
				customerId);
		Long exp = null;
		if (validityMillis != null) {
			exp = (creationTimestamp == null) ? System.currentTimeMillis()
					: creationTimestamp;
			exp += validityMillis;
		}
		AccessTokenPK tokenId = new AccessTokenPK(UUID.randomUUID().toString());
		String clientId = clientPk == null ? null : clientPk.getClientId();
		AccessToken newToken = new AccessToken(tokenId, customerId, clientId,
				exp);
		newToken.setUserId(userId);
		newToken.setType(type);
		newToken.setRefreshToken(refreshTokenId);
		AccessToken token = DAOFactory.getDAOFactory()
				.getDAO(AccessToken.class).create(rootContext, newToken);
		return token;
	}

	public long getResetPasswordTokenExpirationPeriodMillis() {
		return resetPasswordTokenExpirationPeriodMillis;
	}

	public long getTokenExpirationPeriodMillis() {
		return tokenExpirationPeriodMillis;
	}
	
	public long getCodeExpirationPeriodMillis() {
		return codeExpirationPeriodMillis;
	}

	public boolean isDryRunEnabled(HttpServletRequest request) {
		return (request.getParameter(DRY_RUN_PARAM) != null);
	}

	public boolean isNoErrorEnabled(HttpServletRequest request) {
		return (request.getParameter(NO_ERROR_PARAM) != null);
	}

	/**
	 * Retrieve a {@link AccessToken}.
	 * 
	 * @param request
	 *            an HttpServletRequest containing an 'access_token' param.
	 * @return the AccessToken associated to this token or
	 *         <tt>null</null> if none found.
	 * @throws TokenExpiredException
	 *             if the token has expired.
	 */
	public AccessToken getToken(HttpServletRequest request) {
		// try to find from a request param
		String tokenId = (String) request.getParameter(TOKEN_PARAM);
		if (tokenId == null) {
			// try to find from a cookie
			Cookie[] cookies = request.getCookies();
			if (cookies != null) {
				for (int i = 0; i < cookies.length; i++) {
					if (cookies[i].getName().equals(SQUIDAPITOKEN)) {
						tokenId = cookies[i].getValue();
					}
				}
			}
		}
		if (tokenId == null) {
			// try with Bearer header
			Enumeration<String> headers = request.getHeaders(AUTHORIZATION);
			while(headers.hasMoreElements()) {
				String auth = headers.nextElement();
				int idx = auth.indexOf(BEARER_HEADER);
				if (idx>-1) {
					tokenId = auth.substring(BEARER_HEADER.length());
				}
			}
		}
		try {
			AccessToken token = getToken(tokenId);
			if (token != null) {
				return token;
			} else {
				// no token id found
				throw new InvalidTokenAPIException("Auth failed : invalid "
						+ TOKEN_PARAM, isNoErrorEnabled(request));
			}
		} catch (TokenExpiredException e) {
			throw new InvalidTokenAPIException("Auth failed : expired "
					+ TOKEN_PARAM, isNoErrorEnabled(request));
		}
	}

	public String getLocale(HttpServletRequest request) {
		// try to find from a request param
		String locale = (String) request.getParameter(LOCALE_PARAM);
		if (locale == null) {
			// try to find from a cookie
			Cookie[] cookies = request.getCookies();
			if (cookies != null) {
				for (int i = 0; i < cookies.length; i++) {
					if (cookies[i].getName().equals(SQUIDAPILOCALE)) {
						locale = cookies[i].getValue();
					}
				}
			}
		}
		// check string validity
		if ((locale != null) && (locale.length() != 5)) {
			throw new APIException("Invalid " + LOCALE_PARAM + " : " + locale,
					isNoErrorEnabled(request));
		}
		return locale;
	}

	/**
	 * Perform API init cleanup tasks. To be executed when starting.
	 */
	public void initAPI(String buildVersionString, int jobPoolSize,
			int temporaryJobMaxAgeInSeconds) {
		this.buildVersionString = buildVersionString;
		AnalysisJobServiceBaseImpl.getInstance().initJobsGC(temporaryJobMaxAgeInSeconds);
		FacetJobServiceBaseImpl.getInstance().initJobsGC(temporaryJobMaxAgeInSeconds);
		StateServiceBaseImpl.getInstance().initGC(temporaryJobMaxAgeInSeconds);
		AuthServiceImpl.getInstance().initGC();

		// ModelObserver
		DataStoreEventBus.getInstance().subscribe(
				MetaModelObserver.getInstance());
		
		// DistributedEventBus
        GlobalEventPublisher.getInstance().start(System.currentTimeMillis());
        DataStoreEventBus.getInstance().subscribe(GlobalEventPublisher.getInstance());
	}

	public void shutdownAPI() {
		AnalysisJobServiceBaseImpl.getInstance().shutdownJobsExecutor();
		FacetJobServiceBaseImpl.getInstance().shutdownJobsExecutor();
		StateServiceBaseImpl.getInstance().stopGC();
		AuthServiceImpl.getInstance().stopGC();
		GlobalEventPublisher.getInstance().stop();
	}

	public String generateUUID() {
		return ObjectId.get().toString();
	}

	public String getBuildVersionString() {
		return buildVersionString;
	}

	public boolean isDefaultLocale(AppContext ctx) {
		if (ctx.getLocale() == null) {
			return true;
		} else {
			Customer customer = getContextCustomer(ctx);
			if (customer.getDefaultLocale() == null) {
				return false;
			} else if (customer.getDefaultLocale().equals(ctx.getLocale())) {
				return true;
			} else {
				return false;
			}
		}
	}

	/**
	 * Perform MD5 Hashing.
	 * 
	 * @param salt
	 *            String used as MD5 salt
	 * @param input
	 *            String to hash
	 * @return hashed String or inpout (non-hashed) String if salt was
	 *         <code>null</code>
	 */
	public String md5(String salt, String input) {
		String md5 = null;
		if (null == input) {
			return null;
		}
		if (salt == null) {
			return input;
		} else {
			input += salt;
		}
		// digest
		try {
			// Create MessageDigest object for MD5
			MessageDigest digest = MessageDigest.getInstance("MD5");

			// Update input string in message digest
			digest.update(input.getBytes(), 0, input.length());

			// Converts message digest value in base 16 (hex)
			md5 = new BigInteger(1, digest.digest()).toString(16);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		return md5;
	}

	/**
	 * Convert Date to ISO 8601 String.
	 * 
	 * @param date
	 * @return
	 */
	@Deprecated
	public String toISO8601(Date date) {
		DateFormat df = new SimpleDateFormat(ISO8601);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		String formatted = df.format(date);
		return formatted;
	}

	/**
	 * Convert ISO 8601 (javascript) string to Date.
	 */
	public Date toDate(String iso8601string) throws ScopeException {
		if (iso8601string == null) {
			return null;
		}
		DateFormat df = new SimpleDateFormat(ISO8601);
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		// fix for javascript toISOString
		int z = iso8601string.indexOf('Z');
		if (z > 0) {
			iso8601string = iso8601string.substring(0, z) + "+0000";
		}
		try {
			Date date = df.parse(iso8601string);
			return date;
		} catch (ParseException e) {
			// try the short version
			DateFormat dfshort = new SimpleDateFormat(ISO8601short);
			dfshort.setTimeZone(TimeZone.getTimeZone("UTC"));
			try {
				return dfshort.parse(iso8601string);
			} catch (ParseException ee) {
				DateFormat lastChance = new SimpleDateFormat(JavaDateToStringFormat);
				lastChance.setTimeZone(TimeZone.getTimeZone("UTC"));
				try {
					return lastChance.parse(iso8601string);
				} catch (ParseException eee) {
					throw new ScopeException("unable to parse date: \""+iso8601string+"\", supported formats are ISO8601 (\""+ISO8601+"\" or \""+ISO8601short+"\") or Java format (\""+JavaDateToStringFormat+"\")", eee);
				}
			}
		}
	}

	/**
	 * Get the Customer associated to an AppContext (using Root Context).
	 * 
	 * @param ctx
	 * @return Customer
	 */
	public Customer getContextCustomer(AppContext ctx) {
		String customerId = ctx.getCustomerId();
		Customer customer = DAOFactory
				.getDAOFactory()
				.getDAO(Customer.class)
				.readNotNull(
						ServiceUtils.getInstance().getRootUserContext(
								customerId), new CustomerPK(customerId));
		return customer;
	}

	public boolean matchPassword(AppContext ctx, User user, String password) {
		Customer customer = getContextCustomer(ctx);
		return matchPassword(customer, user, password);
	}
	
	public boolean matchPassword(Customer customer, User user, String password) {
		String hashedPassword = ServiceUtils.getInstance().md5(
				customer.getMD5Salt(), password);
		return user.getPassword().equals(hashedPassword);
	}

	public String encrypt(String key, String data) throws Exception {
		Key keySpec = new SecretKeySpec(key.getBytes(), ALGO);
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.ENCRYPT_MODE, keySpec);
		byte[] encVal = c.doFinal(data.getBytes());
		String encryptedValue = DatatypeConverter.printBase64Binary(encVal);
		return encryptedValue;
	}

	public String decrypt(String key, String encryptedData) throws Exception {
		Key keySpec = new SecretKeySpec(key.getBytes(), ALGO);
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.DECRYPT_MODE, keySpec);
		byte[] decordedValue = DatatypeConverter
				.parseBase64Binary(encryptedData);
		byte[] decValue = c.doFinal(decordedValue);
		String decryptedValue = new String(decValue);
		return decryptedValue;
	}
	
	/**
	 * Log an API request. This method currently logs out to a specific logger,
	 * but it could also be a good place to implement per user/client api call
	 * rate limits.
	 * 
	 * @param ctx
	 * @param request
	 */
	public void logAPIRequest(AppContext ctx, HttpServletRequest req) {
		String customerId = null;
		String clientId = null;
		String userId = null;
		String userLogin = null;
		if (ctx != null) {
			customerId = ctx.getCustomerId();
			if (ctx.getUser() != null) {
				userId = ctx.getUser().getId().getUserId();
				userLogin = ctx.getUser().getLogin();	
			}
			clientId = ctx.getClientId();
		}
		logAPIRequest(customerId, clientId, userId, userLogin, req);
		
	}

	/**
	 * Log an API request. This method currently logs out to a specific logger,
	 * but it could also be a good place to implement per user/client api call
	 * rate limits.
	 * 
	 * @param customerId
	 * @param clientId
	 * @param userId
	 * @param userLogin
	 * @param request
	 */
	public void logAPIRequest(String customerId, String clientId,
			String userId, String userLogin, HttpServletRequest req) {
		// log the request along with the user
		StringBuilder log = new StringBuilder();
		log.append("customerId:").append(customerId).append('\t');
		log.append("clientId:").append(clientId).append('\t');
		log.append("userId:").append(userId);
		if (userLogin != null) {
			log.append('(').append(userLogin).append(')');
		}
		log.append('\t');

		String reqIp = getRemoteIP(req);
		String reqMethod = req.getMethod();
		String reqPath = req.getPathInfo();
		String reqQuery = req.getQueryString();

		log.append("request:").append(reqIp).append(' ').append(reqMethod)
				.append(' ').append(reqPath);
		if (reqQuery != null) {
			log.append('?').append(reqQuery);
		}

		loggerAPI.info(log.toString());
	}
	
	/**
	 * Get the "real" remote host IP using X-Forwarded-For http header.
	 * @param req
	 * @return forwarded IP or remote host if no xff header found
	 */
	public String getRemoteIP(HttpServletRequest req) {
		String reqIp;
		// check if we have a X-Forwarded-For header (if we're behind a proxy)
		final String xff = req.getHeader(HEADER_XFF);
		if (xff != null) {
			// default if xff
			reqIp = xff;
			// get the right-most non private IP address
			StringTokenizer st = new StringTokenizer(xff, ",");
			while (st.hasMoreTokens()) {
				String tok = StringUtils.trim(st.nextToken());
				if ((!tok.startsWith(PRIVATE_IP_PREFIX_AWS))
						&& (!tok.startsWith(PRIVATE_IP_PREFIX_SQUID))) {
					reqIp = tok;
				}
			}
		} else {
			// use the normal remote IP
			reqIp = req.getRemoteAddr();
		}
		return reqIp;
	}

	/**
	 * Guess the public base URI for the server
	 * @param uriInfo2
	 * @return
	 */
	public URI guessPublicBaseUri(UriInfo uriInfo) {
		// first check if there is a publicBaseUri parameter
		String uri = KrakenConfig.getProperty(KrakenConfig.publicBaseUri, true);
		if (uri!=null) {
			try {
				return new URI(uri);
			} catch (URISyntaxException e) {
				// let's try the next
			}
		}
		// second, try to use the OAuth endpoint
		String oauthEndpoint = KrakenConfig.getProperty(KrakenConfig.krakenOAuthEndpoint,true);
		if (oauthEndpoint!=null) {
			try {
				URI check = new URI(oauthEndpoint);
				// check that it is not using the ob.io central auth
				if (!"auth.openbouquet.io".equalsIgnoreCase(check.getHost())) {
					while (oauthEndpoint.endsWith("/")) {
						oauthEndpoint = oauthEndpoint.substring(0, oauthEndpoint.length()-1);
					}
					if (oauthEndpoint.endsWith("/auth/oauth")) {
						oauthEndpoint = oauthEndpoint.substring(0,oauthEndpoint.length() - "/auth/oauth".length());
						return new URI(oauthEndpoint+"/v4.2");
					}
				}
			} catch (URISyntaxException e) {
				// let's try the next
			}
		}
		// last, use the uriInfo
		return uriInfo.getBaseUri();
	}
}
