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

import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.mail.MessagingException;

import org.apache.commons.codec.binary.Base64;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.EmailHelper;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.InvalidTokenAPIException;
import com.squid.kraken.v4.api.core.ModelGC;
import com.squid.kraken.v4.api.core.OBioApiHelper;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.AccessToken.Type;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.AuthCode;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.CustomerInfo;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStoreQueryField;
import com.squid.kraken.v4.persistence.dao.AccessTokenDAO;
import com.squid.kraken.v4.persistence.dao.ClientDAO;
import com.squid.kraken.v4.persistence.dao.UserDAO;

import io.openbouquet.api.model.Membership;

public class AuthServiceImpl extends
		GenericServiceImpl<AccessToken, AccessTokenPK> {

	final Logger logger = LoggerFactory.getLogger(AuthServiceImpl.class);

	private ScheduledExecutorService modelGC;

	private ScheduledFuture<?> modelGCThread;

	private static AuthServiceImpl instance;

	public static AuthServiceImpl getInstance() {
		if (instance == null) {
			instance = new AuthServiceImpl();
		}
		return instance;
	}

	protected DAOFactory factory = DAOFactory.getDAOFactory();

	private AuthServiceImpl() {
		// made private for singleton access
		super(AccessToken.class);
	}

	public void initGC() {
		modelGC = Executors.newSingleThreadScheduledExecutor();
		ModelGC<AccessToken, AccessTokenPK> gc = new ModelGC<AccessToken, AccessTokenPK>(
				0, this, AccessToken.class);
		modelGCThread = modelGC
				.scheduleWithFixedDelay(gc, 0, 1, TimeUnit.HOURS);
	}

	public void stopGC() {
		try {
			logger.info("stopping GC scheduler for "
					+ this.getClass().getName());
			if (modelGCThread != null) {
				modelGCThread.cancel(true);
			}

			if (modelGC != null) {
				modelGC.shutdown();
				modelGC.awaitTermination(2, TimeUnit.SECONDS);
				modelGC.shutdownNow();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Authenticate a {@link User} and return a new {@link AccessToken}.<br>
	 * Note : Tokens received on the fragment MUST be explicitly validated by
	 * calling /tokeninfo endpoint.<br>
	 * Failure to verify tokens acquired this way makes your application more
	 * vulnerable to the confused deputy problem.
	 * 
	 * @param clientId
	 * @param redirectUrl
	 * @param login
	 *            user login
	 * @param password
	 *            user password
	 * @return an {@link AccessToken} set with default validity
	 */
	public AccessToken authAndReturnToken(AppContext ctx, ClientPK clientId,
			String redirectUrl, String login, String password)
			throws DuplicateUserException {
		User user = auth(ctx, clientId, redirectUrl, login, password);
		return ServiceUtils.getInstance().createToken(user.getCustomerId(), clientId, user.getId()
				.getUserId(), System.currentTimeMillis(), ServiceUtils
				.getInstance().getTokenExpirationPeriodMillis(), null, null);
	}

	/**
	 * Authenticate a {@link User}.<br>
	 * 
	 * @param ctx
	 * @param clientId
	 * @param redirectUrl
	 * @param login
	 *            user login
	 * @param password
	 *            user password
	 * @return a User
	 */
	public User auth(AppContext ctx, ClientPK clientId, String redirectUrl,
			String login, String password) throws DuplicateUserException {

		if (clientId == null) {
			throw new ObjectNotFoundAPIException("ClientId must be provided",
					ctx.isNoError());
		}
		CustomerServiceBaseImpl customerService = CustomerServiceBaseImpl
				.getInstance();
		String customerId = ctx.getCustomerId();
		if ((customerId == null) || customerId.isEmpty()) {
			// Build a custom query to search for users across all customers
			AppContext emptyContext = new AppContext.Builder().build();
			List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
			queryFields.add(new DataStoreQueryField("login", login
					.toLowerCase()));
			List<User> users = DAOFactory.getDAOFactory().getBaseDataStore()
					.find(emptyContext, User.class, queryFields, null);
			List<User> usersMatched = new ArrayList<User>();

			if (users.size() == 0) {
				logger.info("login failed (user not found) for user-login : '"
						+ login + "'");
				throw new ObjectNotFoundAPIException(
						"User not found for given login / password (ERR1)",
						ctx.isNoError());
			}

			// filter by password matching
			String loginExceptionMessage = null;
			for (User user : users) {
				// Get the Customer using root context.
				CustomerPK cId = new CustomerPK(user.getCustomerId());
				AppContext root = ServiceUtils.getInstance()
						.getRootUserContext(cId.getCustomerId());
				Customer customerPrivate = customerService.read(root, cId);

				if (ServiceUtils.getInstance().matchPassword(customerPrivate,
						user, password)) {
					// check the client before adding the the matched users
					ClientPK clientIdtmp = new ClientPK(
							user.getCustomerId(), clientId.getClientId());
					try {
						verifyClient(clientIdtmp, redirectUrl, ctx.isNoError());
						usersMatched.add(user);
					} catch (RuntimeException e) {
						loginExceptionMessage = e.getMessage();
						logger.info("Client check failed for user "+ user.getLogin() +" : "+ loginExceptionMessage);
					}
				} else {
					loginExceptionMessage = "Password check failed";
				}
			}

			if (usersMatched.size() == 0) {
				if (users.size() <= 1) {
					loginExceptionMessage = "User not found for given login / password (ERR2)";
				}
				logger.info("Invalid login for user "+ login +" : "+loginExceptionMessage);
				throw new ObjectNotFoundAPIException(loginExceptionMessage,
						ctx.isNoError());
			} else if (usersMatched.size() > 1) {
				// multiple users found with same login/pwd
				List<CustomerInfo> customers = new ArrayList<CustomerInfo>();
				for (User user : usersMatched) {
					customers.add(customerService.readCustomerInfo(user
							.getCustomerId()));
				}
				throw new DuplicateUserException(ctx.isNoError(), customers);
			} else {
				// unique user found
				customerId = usersMatched.get(0).getCustomerId();
			}
		}

		User user = checkLogin(ctx, customerId, clientId.getClientId(),
				redirectUrl, login, password);
		return user;
	}

	/**
	 * Authenticate a {@link User} and return a new {@link AuthCode}.<br>
	 * 
	 * @param userContext
	 *            .getCustomerId()
	 * @param clientId
	 * @param redirectUrl
	 * @param login
	 *            user login
	 * @param password
	 *            user password
	 * @return an {@link AuthCode}
	 */
	public AuthCode authAndReturnCode(AppContext anonymousCtx,
			ClientPK clientId, String redirectUrl, String login,
			String password, boolean generateRefreshToken) {
		User user = auth(anonymousCtx, clientId, redirectUrl, login, password);
		String refreshTokenId;
		if (generateRefreshToken) {
			AccessTokenDAO dao = (AccessTokenDAO) DAOFactory.getDAOFactory()
					.getDAO(AccessToken.class);
			// create a refresh token if not already existing
			Optional<AccessToken> findRefreshToken = dao
					.findRefreshToken(user.getCustomerId(),
							clientId.getClientId(), user.getOid());
			AccessToken refreshToken;
			if (findRefreshToken.isPresent()) {
				refreshToken = findRefreshToken.get();
			} else {
				refreshToken = createRefreshToken(user.getCustomerId(), clientId, user
						.getId().getUserId());
			}
			// store its id in the authCode
			refreshTokenId = refreshToken.getId().getTokenId();
		} else {
			refreshTokenId = null;
		}
		AccessToken authCode = createAuthCode(user.getCustomerId(), clientId, user
				.getId().getUserId(), refreshTokenId);
		return new AuthCode(authCode.getId().getTokenId());
	}

	/**
	 * Exchange an authorization code with an access token.<br>
	 * 
	 * @param ctx
	 * @param clientId
	 * @param redirectUrl
	 * @param authorizationCode
	 */
	public AccessToken getTokenFromAuthCode(AppContext ctx, ClientPK clientId,
			String redirectUrl, String authorizationCode) {
		AccessToken codeToken;
		AccessToken token = null;

		// get code token
		codeToken = ServiceUtils.getInstance().getToken(authorizationCode, clientId.getClientId());
		// create a new access token
		token = ServiceUtils.getInstance().createToken(codeToken.getCustomerId(), clientId,
				codeToken.getUserId(), System.currentTimeMillis(), ServiceUtils
						.getInstance().getTokenExpirationPeriodMillis(), null, null);
		
		// set refresh token
		if (codeToken.getRefreshToken() != null) {
			token.setRefreshToken(codeToken.getRefreshToken());
		}

		// delete the codeToken
		DAOFactory.getDAOFactory().getDAO(AccessToken.class)
				.delete(ctx, codeToken.getId());

		return token;
	}
	
	/**
	 * Exchange an authorization code with an access token.<br>
	 * 
	 * @param ctx
	 * @param clientId
	 * @param redirectUrl
	 * @param authorizationCode
	 * @param teamId
	 */
	public AccessToken getTokenFromOBioAuthCode(AppContext ctx, ClientPK clientId,
			String redirectUrl, String authorizationCode, String teamId) {
		AccessToken token;
		if ((KrakenConfig.getAuthMode() == AUTH_MODE.BYPASS) || (KrakenConfig.getAuthMode() == AUTH_MODE.OBIO)) {
			// look for a Customer that has a a bypass auth mode
			Customer singleCustomer = ServiceUtils.getInstance().getSingleCustomer();
			if ((singleCustomer != null) && (authorizationCode != null)) {
				// Perform Auth with OB.io
				Membership membership = null;
				try {
					if (teamId !=null) {
						// get User membership for the team
						membership = OBioApiHelper.getInstance().getMembershipService().get("Bearer "+authorizationCode, teamId);
					} else {
						// get User memberships
						List<Membership> userMemberships = OBioApiHelper.getInstance().getMembershipService().getUserMemberships("Bearer "+authorizationCode);
						// check if there is a suitable membership for current Customer
						for (Membership m : userMemberships) {
							if ((m.getTeam() != null) && (m.getTeam().getId().equals(singleCustomer.getTeamId()))) {
								membership = m;
							}
						}
					}
				} catch (Exception e) {
					logger.info("Auth with OB.io failed", e);
					throw new InvalidTokenAPIException("Auth failed", false, KrakenConfig.getAuthServerEndpoint());
				}
				if (membership != null) {
					String userId = null;
					AppContext root = ServiceUtils.getInstance().getRootUserContext(singleCustomer.getCustomerId());
					if ((singleCustomer.getTeamId() == null) || (singleCustomer.getAuthMode() != AUTH_MODE.OBIO)) {
						// register the customer
						logger.info("Registering Customer with Team : " + membership.getTeam().getId());
						singleCustomer.setTeamId(membership.getTeam().getId());
						singleCustomer.setPublicUrl(membership.getTeam().getServerUrl());
						singleCustomer.setAuthMode(AUTH_MODE.OBIO);
						CustomerServiceBaseImpl.getInstance().store(root, singleCustomer);
					} else if (!singleCustomer.getTeamId().equals(membership.getTeam().getId())) {
						// this membership does not allow to access
						logger.info("User's Team (" + membership.getTeam().getId()
								+ ") does not allow to access this Customer's Team (" + singleCustomer.getTeamId()
								+ ")");
						throw new InvalidTokenAPIException("Auth failed : invalid membership", false,
								KrakenConfig.getAuthServerEndpoint());
					}
					// User registration
					io.openbouquet.api.model.User userOBio = membership.getUser();
					UserDAO userDAO = ((UserDAO) DAOFactory.getDAOFactory().getDAO(User.class));
					// authId holds the obio userId
					Optional<User> userOpt;
					userOpt = userDAO.findByAuthId(root, userOBio.getId());
					if (userOpt.isPresent()) {
						// user already registered
						userId = userOpt.get().getOid();
					} else if (userOBio.getEmail() != null) {
						userOpt = userDAO.findByEmail(root, userOBio.getEmail());
						if (userOpt.isPresent()) {
							// we have a user with matching email
							User user = userOpt.get();
							user.setLogin(userOBio.getName());
							user.setAuthId(userOBio.getId());
							userDAO.update(root, user);
							userId = user.getOid();
						}
					}
					if (userId == null) {
						List<User> findByCustomer = userDAO.findByCustomer(root, singleCustomer.getId());
						if (findByCustomer.size() == 1) {
							// we only have one non registered user
							User user = findByCustomer.get(0);
							user.setLogin(userOBio.getName());
							user.setAuthId(userOBio.getId());
							user.setEmail(userOBio.getEmail());
							userDAO.update(root, user);
							userId = user.getOid();
						} else {
							// register a brand new user
							User user = new User();
							user.setId(new UserPK(singleCustomer.getCustomerId()));
							user.setLogin(userOBio.getName());
							user.setAuthId(userOBio.getId());
							user.setEmail(userOBio.getEmail());
							user = userDAO.create(root, user);
							userId = user.getOid();
						}
					}
					// generate a new token
					ClientPK client = new ClientPK(singleCustomer.getCustomerId(), clientId.getClientId());
					token = ServiceUtils.getInstance().createToken(singleCustomer.getCustomerId(), client, userId,
							System.currentTimeMillis(), ServiceUtils.getInstance().getTokenExpirationPeriodMillis(),
							AccessToken.Type.NORMAL, null, authorizationCode);
				} else {
					logger.info("Auth with OB.io failed : no Membership");
					throw new InvalidTokenAPIException("Auth failed", false, KrakenConfig.getAuthServerEndpoint());
				}
			} else {
				logger.info("Auth with OB.io failed : invalid code or more that one customer found");
				throw new InvalidTokenAPIException("Auth failed", false, KrakenConfig.getAuthServerEndpoint());
			}
		} else {
			logger.info("Auth with OB.io failed : invalid Auth Mode");
			throw new InvalidTokenAPIException("Auth failed", false, KrakenConfig.getAuthServerEndpoint());
		}

		return token;
	}
	
	public AccessToken getTokenFromRefreshToken(AppContext ctx, ClientPK clientId, String clientSecret,
			String redirectUrl, String refreshTokenValue) {
		AccessTokenDAO dao = (AccessTokenDAO) DAOFactory.getDAOFactory()
				.getDAO(AccessToken.class);
		AccessToken refreshToken;
		Optional<AccessToken> findRefreshToken = dao
				.findRefreshToken(refreshTokenValue);

		// check token
		// TODO check redirect URL & Client Secret
		if (!findRefreshToken.isPresent()) {
			throw new InvalidCredentialsAPIException("Invalid refresh token (ERR0)",
					ctx.isNoError());
		} else {
			refreshToken = findRefreshToken.get();
		}
		
		if (!refreshToken.getClientId().equals(clientId.getClientId())) {
			throw new InvalidCredentialsAPIException("Invalid refresh token (ERR1)",
					ctx.isNoError());
		}

		// create a new access token
		AccessToken token = ServiceUtils.getInstance().createToken(refreshToken.getCustomerId(), clientId,
				refreshToken.getUserId(), System.currentTimeMillis(), ServiceUtils
						.getInstance().getTokenExpirationPeriodMillis(), null, null);
		token.setRefreshToken(refreshToken.getOid());

		return token;
	}
	
	public AccessToken getTokenFromJWT(AppContext ctx, String jwt) {
		try {
			// first pass to read the issuer (client Id)
			JwtConsumer firstPassJwtConsumer = new JwtConsumerBuilder()
			.setSkipAllValidators()
			.setDisableRequireSignature()
			.setSkipSignatureVerification()
			.build();
			JwtContext jwtContext = firstPassJwtConsumer.process(jwt);
			JwtClaims claims = jwtContext.getJwtClaims();
			String issuer = claims.getIssuer();
			String customerId = claims.getStringClaimValue("customerId");
			ClientPK clientId = new ClientPK(claims.getStringClaimValue("customerId"), issuer);
			
			// load the client using superuser to get the key
			AppContext rootUserContext = ServiceUtils.getInstance().getRootUserContext(customerId);
			Client client = DAOFactory.getDAOFactory().getDAO(Client.class).readNotNull(rootUserContext, clientId);
			String publicKeyPEM = client.getJWTKeyPublic();
			publicKeyPEM = publicKeyPEM.substring(publicKeyPEM.indexOf('\n'), publicKeyPEM.lastIndexOf('\n'));
			publicKeyPEM = publicKeyPEM.replace("\n", "");
			byte[] publicKey = Base64.decodeBase64(publicKeyPEM);
			
			KeySpec keySpec = new X509EncodedKeySpec(publicKey);
		    KeyFactory kf = KeyFactory.getInstance("RSA");
		    PublicKey key = kf.generatePublic(keySpec);
		    
			JwtConsumer jwtConsumer = new JwtConsumerBuilder()
			.setRequireExpirationTime() // the JWT must have an expiration time
			.setAllowedClockSkewInSeconds(30) // allow some leeway in validating time based claims to account for clock skew
			.setRequireSubject() // the JWT must have a subject claim
			.setVerificationKey(key) // verify the signature with the public key
			.build(); // create the JwtConsumer instance
			
			// validate the JWT
			jwtConsumer.processContext(jwtContext);
			
			// create the token
			String userId = jwtContext.getJwtClaims().getSubject();
			AccessToken token = ServiceUtils.getInstance().createToken(customerId, clientId,
					userId, System.currentTimeMillis(), ServiceUtils
							.getInstance().getTokenExpirationPeriodMillis(), null, null);
			return token;
		} catch (MalformedClaimException e) {
			logger.debug(e.getMessage());
			throw new InvalidCredentialsAPIException("Invalid JWT Claim", ctx.isNoError());
		} catch (InvalidJwtException e) {
			logger.debug(e.getMessage());
			throw new InvalidCredentialsAPIException("Invalid JWT", ctx.isNoError());
		} catch (NoSuchAlgorithmException e) {
			logger.debug(e.getMessage());
			throw new RuntimeException(e);
		} catch (InvalidKeySpecException e) {
			logger.debug(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	/**
	 * Check {@link User} login.<br>
	 * Redirect URL check will be performed to make sure its domain matches a
	 * least one of the Client URLs (bypassed if no client URLS defined).
	 * 
	 * @param userContext
	 *            .getCustomerId()
	 * @param clientId
	 * @param redirectUrl
	 * @param login
	 *            user login
	 * @param password
	 *            user password
	 * @return a {@link User}
	 */
	private User checkLogin(AppContext ctx, String customerId, String clientId,
			String redirectUrl, String login, String password) {

		AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(
				customerId);

		ClientPK clientPk = new ClientPK(customerId, clientId);
		try {
			verifyClient(clientPk, redirectUrl, rootctx.isNoError());
		} catch (RuntimeException e) {
			logger.info(e.getMessage());
			throw e;
		}

		// user login lookup
		UserDAO userDAO = (UserDAO) factory.getDAO(User.class);
		Optional<User> userOpt = userDAO.findByLogin(rootctx, login);

		if (userOpt.isPresent()) {
			User user = userOpt.get();
			if (ServiceUtils.getInstance().matchPassword(rootctx, user,
					password)) {
				if (!user.isSuperUser()) {
					logger.info("login(" + customerId + "," + clientId + ","
							+ user.getId().getUserId() + ")");
					return user;
				} else {
					logger.info("login failed (super user) for user-login : '"
							+ login + "' [customerId :" + customerId + "]");
					throw new InvalidCredentialsAPIException(
							"sorry, this user cannot log-in", ctx.isNoError());
				}
			} else {
				logger.info("login failed (invalid pwd) for user-login : '"
						+ login + "' [customerId :" + customerId + "]");
			}
		} else {
			logger.info("login failed (user not found) for user-login : '"
					+ login + "' [customerId :" + customerId + "]");
		}
		throw new ObjectNotFoundAPIException(
				"User not found for given login / password", ctx.isNoError());
	}

	public void logoutUser(AppContext ctx) {
		logger.info(ctx.getUser() + " logoutUser");
		AccessTokenDAO dao = ((AccessTokenDAO) DAOFactory.getDAOFactory()
				.getDAO(AccessToken.class));
		List<AccessToken> findByUser = dao.findByUser(ctx, ctx.getUser()
				.getId());
		for (AccessToken token : findByUser) {
			dao.delete(ctx, token.getId());
		}
	}

	private void verifyClient(ClientPK clientId, String redirectUrl,
			boolean isNoError) {
		String serverMode = KrakenConfig
				.getProperty("kraken.server.mode", true);
		if ((serverMode != null) && (serverMode.equals("dev"))) {
			logger.info("DevMode : bypassing clientId check");
			return;
		}

		if (clientId != null) {
			// client lookup
			ClientDAO clientDAO = (ClientDAO) factory.getDAO(Client.class);
			Client client;
			AppContext ctx = ServiceUtils.getInstance().getRootUserContext(
					clientId.getCustomerId());
			Optional<Client> clientOpt = clientDAO.read(ctx, clientId);
			if (!clientOpt.isPresent()) {
				throw new ObjectNotFoundAPIException("Client Id not found", isNoError);
			} else {
				client = clientOpt.get();
			}

			if (redirectUrl != null) {
				// redirect url validation
				try {
					boolean ok = false;
					if (client.getUrls().isEmpty()) {
						ok = true;
					} else {
						URL redirect = new URL(redirectUrl);
						for (String urlS : client.getUrls()) {
							try {
								if (urlS.equals(redirect.getHost())) {
									ok = true;
								}
							} catch (Exception e) {
								// should not happen
								logger.warn(e.getMessage(), e);
							}
						}
					}
					if (!ok) {
						throw new InvalidCredentialsAPIException(
								"Redirect URL not allowed",
								isNoError);
					}
				} catch (MalformedURLException e) {
					throw new APIException("Redirect URL is malformed", isNoError);
				}
			}
		} else {
			throw new APIException("Client id must be provided", isNoError);
		}
	}

	public List<AccessToken> resetUserPassword(AppContext anonymousCtx,
			EmailHelper emailHelper, String clientId, String email,
			String lang, String url, String emailContent, String emailSubject) {
		// findUserByEmail
		if (email == null) {
			throw new ObjectNotFoundAPIException(
					"User not found for given email", anonymousCtx.isNoError());
		}

		List<User> users;
		String customerId = anonymousCtx.getCustomerId();
		AppContext emptyContext = new AppContext.Builder().build();
		if ((customerId == null) || customerId.isEmpty()) {
			// Build a custom query to search for users across all customers
			List<DataStoreQueryField> queryFields = new LinkedList<DataStoreQueryField>();
			queryFields.add(new DataStoreQueryField("email", email
					.toLowerCase()));
			users = DAOFactory.getDAOFactory().getBaseDataStore()
					.find(emptyContext, User.class, queryFields, null);

			if (users.size() == 0) {
				logger.info("user not found for user-email : '" + email + "'");
				throw new ObjectNotFoundAPIException(
						"User not found for given email",
						anonymousCtx.isNoError());
			}
		} else {
			AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(
					customerId);
			UserDAO userDAO = (UserDAO) factory.getDAO(User.class);
			Optional<User> userOpt = userDAO.findByEmail(rootctx, email);
			if (userOpt.isPresent()) {
				users = new ArrayList<User>();
				users.add(userOpt.get());
			} else {
				logger.info("user not found for user-email : '" + email
						+ "' and customer : " + customerId);
				throw new ObjectNotFoundAPIException(
						"User not found for given email",
						anonymousCtx.isNoError());
			}
		}

		String resetLink = "";
		List<AccessToken> tokens = new ArrayList<AccessToken>();
		for (User user : users) {
			String custId = user.getCustomerId();
			AppContext rootctx = ServiceUtils.getInstance().getRootUserContext(
					custId);
			CustomerInfo customerInfo = CustomerServiceBaseImpl.getInstance()
					.readCustomerInfo(rootctx);

			ClientPK clientPk = new ClientPK(custId, clientId);
			try {
				verifyClient(clientPk, url, rootctx.isNoError());
			} catch (RuntimeException e) {
				logger.info(e.getMessage());
				clientPk = null;
			}

			// createToken
			if (clientPk != null) {
				AccessToken token = ServiceUtils.getInstance().createToken(custId, clientPk, user.getId()
						.getUserId(), System.currentTimeMillis(), ServiceUtils
						.getInstance().getResetPasswordTokenExpirationPeriodMillis(),
						AccessToken.Type.RESET_PWD, null);
				tokens.add(token);

				// processResetPasswordTemplate
				String link = url.replace('{' + ServiceUtils.TOKEN_PARAM + "}",
						token.getId().getTokenId());

				try {
					new URL(link);
				} catch (MalformedURLException e1) {
					throw new APIException("Invalid link url", e1,
							anonymousCtx.isNoError());
				}
				if (users.size() > 1) {
					String customerName = customerInfo.getName();
					if (customerName == null) {
						customerName = "id:" + customerInfo.getId();
					}
					resetLink += "Customer '" + customerName + "' : " + link
							+ "\n";
				} else {
					resetLink = link;
				}
			}
		}

		if (tokens.size() == 0) {
			logger.info("No valid user found for user-email : '" + email
					+ "' and customer : " + customerId);
			throw new ObjectNotFoundAPIException(
					"User not found for given email", anonymousCtx.isNoError());
		} else {
			emailContent = emailContent.replace("${validity}", ""
					+ (ServiceUtils.getInstance()
							.getTokenExpirationPeriodMillis() / 3600000));
			emailContent = emailContent.replace("${resetLink}", resetLink);

			// sendMail
			List<String> dests = Arrays.asList(email);
			try {
				logger.info("Sending password reset token to " + email);
				emailHelper.sendEmail(dests, emailSubject, emailContent, null,
						EmailHelper.PRIORITY_NORMAL);
				return tokens;
			} catch (MessagingException e) {
				throw new APIException(e, anonymousCtx.isNoError());
			}
		}

	}

	private AccessToken createRefreshToken(String customerId, ClientPK clientPk,
			String userId) {
		return ServiceUtils.getInstance().createToken(customerId, clientPk, userId, System.currentTimeMillis(), null, Type.REFRESH, null);
	}
	
	private AccessToken createAuthCode(String customerId, ClientPK clientPk,
			String userId, String refreshTokenId) {
		return ServiceUtils.getInstance().createToken(customerId, clientPk, userId, System.currentTimeMillis(), ServiceUtils
				.getInstance().getTokenExpirationPeriodMillis(), Type.CODE, refreshTokenId);
	}

}
