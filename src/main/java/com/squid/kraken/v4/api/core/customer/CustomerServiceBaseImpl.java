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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import javax.mail.MessagingException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.CoreConstants;
import com.squid.kraken.v4.api.core.EmailHelper;
import com.squid.kraken.v4.api.core.EmailHelperImpl;
import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.user.UserServiceBaseImpl;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.CustomerInfo;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

public class CustomerServiceBaseImpl extends
		GenericServiceImpl<Customer, CustomerPK> {

	final Logger logger = LoggerFactory
			.getLogger(CustomerServiceBaseImpl.class);

	private static CustomerServiceBaseImpl instance;

	public static CustomerServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new CustomerServiceBaseImpl();
		}
		return instance;
	}

	protected DAOFactory factory = DAOFactory.getDAOFactory();

	private CustomerServiceBaseImpl() {
		// made private for singleton access
		super(Customer.class);
	}

	public CustomerInfo readCustomerInfo(AppContext ctx) {
		// Get the Customer using user context.
		Customer customerUser;
		try {
			customerUser = read(ctx, ctx.getCustomerPk());
		} catch (InvalidCredentialsAPIException e) {
			customerUser = null;
		}

		CustomerInfo customerInfo = readCustomerInfo(ctx.getCustomerPk()
				.getCustomerId());
		
		AccessRightsUtils.getInstance().setRole(ctx, customerInfo);

		// set the AWS customer Id
		if (customerUser != null) {
			customerInfo.setAWSClientId(customerUser.getAWSClientId());
			customerInfo.setClients(customerUser.getClients());
			customerInfo.setProjects(customerUser.getProjects());
			customerInfo.setUserGroups(customerUser.getUserGroups());
			customerInfo.setUsers(customerUser.getUsers());
			customerInfo.setShortcuts(customerUser.getShortcuts());
			customerInfo.setStates(customerUser.getStates());
		}

		return customerInfo;
	}

	protected CustomerInfo readCustomerInfo(String customerId) {
		// Get the Customer using root context.
		AppContext root = ServiceUtils.getInstance().getRootUserContext(
				customerId);
		Customer customerPrivate = read(root, new CustomerPK(customerId));
		CustomerInfo customerInfo = new CustomerInfo(customerPrivate);
		return customerInfo;
	}

	public CustomerInfo updateCustomerInfo(AppContext ctx,
			CustomerInfo customerInfo) {
		Customer cust = factory.getDAO(Customer.class).readNotNull(ctx,
				ctx.getCustomerPk());
		cust.setAWSClientId(customerInfo.getAWSClientId());
		cust.setName(customerInfo.getName());
		cust.setDefaultLocale(customerInfo.getDefaultLocale());
		cust.setClients(customerInfo.getClients());
		cust.setProjects(customerInfo.getProjects());
		cust.setUsers(customerInfo.getUsers());
		cust.setUserGroups(customerInfo.getUserGroups());
		cust.setShortcuts(customerInfo.getShortcuts());
		cust.setStates(customerInfo.getStates());
		cust.setAuthMode(customerInfo.getAuthMode());
		super.store(ctx, cust);
		return readCustomerInfo(ctx);
	}

	public User readContextUser(AppContext ctx) {
		return ctx.getUser();
	}

	// non REST API methods

	/**
	 * Create (init) a Customer.
	 * <ul>
	 * <li>Create a new Customer.</li>
	 * <li>Create a new User with given rights on Customer.</li>
	 * <li>Create new Clients</li>
	 * </ul>
	 * 
	 * @param customerName
	 *            (can be null)
	 * @param userLogin
	 *            (if null will use email)
	 * @param userPassword
	 * @param userEmail
	 * @param clients
	 * @return AppContext for the User.
	 */
	public AppContext createCustomer(AUTH_MODE authMode, String customerName, String defaultLocale,
			String salt, String userLogin, String userPassword,
			String userEmail, List<Client> clients) {
		Customer customer = new Customer(customerName);
		customer.setDefaultLocale(defaultLocale);
		customer.setMD5Salt(salt);
		customer.setAuthMode(authMode);
		if (StringUtils.isEmpty(userLogin)) {
			if (!StringUtils.isEmpty(userEmail)) {
				userLogin = userEmail;
			} else {
				throw new IllegalArgumentException("user email and login cannot be both empty");
			}
		}
		return createCustomer(customer, userLogin, userPassword, userEmail,
				clients);
	}

	/**
	 * Create (init) a Customer.
	 * <ul>
	 * <li>Create a new Customer.</li>
	 * <li>Create a new User with given rights on Customer.</li>
	 * <li>Create new Clients</li>
	 * </ul>
	 * 
	 * @param customerName
	 * @param defaultLocale
	 *            for customer
	 * @param salt
	 *            used for user passwords hashing
	 * @param userLogin
	 * @param userPassword
	 * @param userEmail
	 * @param clients
	 * @return AppContext for a new User.
	 */
	public AppContext createCustomer(Customer customer, String userLogin,
			String userPassword, String userEmail, List<Client> clients) {
		// store the customer
		AppContext ctx = ServiceUtils.getInstance().getRootUserContext(
				customer.getCustomerId());
		if (StringUtils.isEmpty(customer.getName())) {
			customer.setName(customer.getCustomerId());
		}
		customer = store(ctx, customer);

		// create the user
		User user = new User(new UserPK(customer.getCustomerId()),
				userLogin.toLowerCase(), userPassword);
		user.setEmail(userEmail);
		user = UserServiceBaseImpl.getInstance().store(ctx, user);

		// customer access rights
		Set<AccessRight> access = customer.getAccessRights();
		access.add(new AccessRight(Role.OWNER, user.getOid(), null));
		// we first set the owner so that UserGroups will be created with this
		// owner
		factory.getDAO(Customer.class).update(ctx, customer);
		access = customer.getAccessRights();

		// create the superusers group
		UserGroup superGroup = DAOFactory
				.getDAOFactory()
				.getDAO(UserGroup.class)
				.create(ctx,
						new UserGroup(new UserGroupPK(ctx.getCustomerId(),
								CoreConstants.CUSTOMER_GROUP_SUPER),
								"Super users"));

		// "superuser" group is assigned OWNER right
		AccessRight owner = new AccessRight();
		owner.setRole(Role.OWNER);
		owner.setGroupId(superGroup.getOid());
		access.add(owner);

		// create the admin group
		UserGroup adminGroup = DAOFactory
				.getDAOFactory()
				.getDAO(UserGroup.class)
				.create(ctx,
						new UserGroup(new UserGroupPK(ctx.getCustomerId(),
								CoreConstants.CUSTOMER_GROUP_ADMIN),
								"Administrators"));

		// "admin" group is assigned WRITE right
		AccessRight admin = new AccessRight();
		admin.setRole(Role.WRITE);
		admin.setGroupId(adminGroup.getOid());
		access.add(admin);

		factory.getDAO(Customer.class).update(ctx, customer);

		// put the user in the superuser group
		user.getGroups().add(superGroup.getOid());
		// do not update the password
		user.setPassword(null);
		user = UserServiceBaseImpl.getInstance().store(ctx, user);

		// create the clients
		for (Client client : clients) {
			client.setCustomerId(customer.getCustomerId());
			Client newClient = DAOFactory.getDAOFactory().getDAO(Client.class).create(ctx, client);
			client.setId(newClient.getId());
		}
		
		// create an AppContext for the first client
		Client client = clients.get(0);
		AppContext anonymousCtx = new AppContext.Builder(
				customer.getCustomerId(), client.getId().getClientId()).build();
		AccessToken token;
		try {
			AuthServiceImpl authService = AuthServiceImpl.getInstance();
			token = authService.authAndReturnToken(anonymousCtx,
					client.getId(), null, userLogin, userPassword);
			return new AppContext.Builder(token, user).build();
		} catch (DuplicateUserException e) {
			// should not happen
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a new Token.
	 * 
	 * @param ctx
	 * @param clientPk
	 * @param userId
	 * @param creationTimestamp
	 *            custom creation date or current date if <tt>null</tt>
	 * @param validityMillis
	 *            the token validity in milliseconds.
	 * @return an AccessToken
	 */
	public AccessToken createToken(AppContext ctx, ClientPK clientPk,
			String userId, Long creationTimestamp, Long validityMillis) {
		long exp = (creationTimestamp == null) ? System.currentTimeMillis()
				: creationTimestamp;
		exp += validityMillis;
		AccessTokenPK tokenId = new AccessTokenPK(UUID.randomUUID().toString());
		String clientId = clientPk == null ? null : clientPk.getClientId();
		AccessToken newToken = new AccessToken(tokenId, ctx.getCustomerId(),
				clientId, exp);
		newToken.setUserId(userId);
		AccessToken token = DAOFactory.getDAOFactory()
				.getDAO(AccessToken.class).create(ctx, newToken);
		return token;
	}

	/**
	 * Request for access to the system. The process will be the following:
	 * <ul>
	 * <li>Create a new Customer with given name.</li>
	 * <li>Create a new User with Owner rights on Customer with given email.</li>
	 * <li>Create 2 new Clients (admin_console and dashboard) with domain "api.squidsolutions.com" for this
	 * Customer (Client.urls - new field in the Client model).</li>
	 * <li>Send a welcome mail including a link to the API Console.</li>
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
	 * @param domain
	 *            the caller domain (remote host)
	 * @param linkURL
	 *            link to return in the email
	 * @param emailHelper
	 *            util to send mail
	 */
	public AppContext accessRequest(AUTH_MODE authMode, String customerName, String email,
			String login, String password, String locale, String domain,
			String linkURL, String defaultClientURL, EmailHelper emailHelper) {

		// set defaults
		if (StringUtils.isEmpty(login) && StringUtils.isEmpty(email)) {
			login = "super";
			password = "super123";
		}
		
		if ((locale != null) && !locale.isEmpty()) {
			locale = locale.trim();
		} else {
			locale = Locale.getDefault().toString();
		}

		List<String> urls = new ArrayList<String>();
		if (defaultClientURL != null) {
			if (defaultClientURL.contains(",")) {
				StringTokenizer st = new StringTokenizer(defaultClientURL, ",");
				while (st.hasMoreElements()) {
					urls.add(st.nextElement().toString());
				}
			} else {
				urls.add(defaultClientURL);
			}
		}
		if (domain != null
				&& ((defaultClientURL == null) || (!defaultClientURL
						.equals(domain)))) {
			urls.add(domain);
		}
		
		// clients
		List<Client> clients = new ArrayList<Client>();
		clients.add(new Client(new ClientPK(null,
				CoreConstants.CONSOLE_CLIENT_ID),
				CoreConstants.CONSOLE_CLIENT_NAME, "" + UUID.randomUUID(), urls));
		clients.add(new Client(new ClientPK(null,
				CoreConstants.DASHBOARD_CLIENT_ID),
				CoreConstants.DASHBOARD_CLIENT_NAME, "" + UUID.randomUUID(),
				urls));

		String salt = UUID.randomUUID().toString();
		AppContext ctx = createCustomer(authMode, customerName, locale, salt, login,
				password, email, clients);

		if (email != null) {
			// send welcome mail
			String linkAccessRequest = linkURL.replace('{'
					+ CoreConstants.PARAM_NAME_CUSTOMER_ID + "}",
					ctx.getCustomerId());
			String content = "Welcome to the SquidAnalytics API.\n\n";
			content += "Your Customer ID is " + ctx.getCustomerId() + "\n\n";
			content += "Please follow this link to access your API Console :\n"
					+ linkAccessRequest;
			content += "\n\nThe SquidAnalytics Team.";
			String subject = "SquidAnalytics API access";
			List<String> dests = Arrays.asList(email);
			try {
				logger.info("Sending API access request link (" + linkAccessRequest
						+ ") to " + email + " " + ctx.getUser());
				List<String> bccAddresses = new ArrayList<String>();
				String bccAddress = KrakenConfig.getProperty("signup.email.bcc",
						true);
				if ((bccAddress != null) && !bccAddress.isEmpty()) {
					bccAddresses.add(bccAddress);
				}
				emailHelper.sendEmail(dests, bccAddresses, subject, content, null,
						EmailHelper.PRIORITY_NORMAL);
			} catch (MessagingException e) {
				throw new APIException(e, ctx.isNoError());
			}
		}

		return ctx;
	}
	
}
