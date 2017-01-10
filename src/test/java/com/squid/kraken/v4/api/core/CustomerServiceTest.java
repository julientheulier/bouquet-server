package com.squid.kraken.v4.api.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.mail.Message;

import org.junit.Ignore;
import org.junit.Test;

import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.customer.AuthServiceImpl;
import com.squid.kraken.v4.api.core.customer.DuplicateUserException;
import com.squid.kraken.v4.api.core.customer.TokenExpiredException;
import com.squid.kraken.v4.api.core.project.ProjectServiceBaseImpl;
import com.squid.kraken.v4.api.core.test.BaseTest;
import com.squid.kraken.v4.model.AccessRight;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.model.CustomerInfo;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;
import com.squid.kraken.v4.model.UserGroupPK;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

@SuppressWarnings({ "unused", "deprecation" })
public class CustomerServiceTest extends BaseTest {

	/**
	 * Create a test Customer with a OWNER user and default (null) Locale and no
	 * salt.
	 * 
	 * @return
	 */
	public static AppContext createTestUserContext() {
		return getTestUserContext((String) null, null);
	}

	/**
	 * Create a test Customer with a OWNER user and no salt.
	 * 
	 * @return
	 */
	public static AppContext getTestUserContext(String defaultLocale) {
		return getTestUserContext(defaultLocale, null);
	}

	/**
	 * Create a test Customer with a OWNER user.
	 * 
	 * @return
	 */
	public static AppContext getTestUserContext(String defaultLocale,
			String salt) {
		return getTestUserContext(defaultLocale, salt, Role.OWNER);
	}

	/**
	 * Create a test Customer with a give user role.
	 * 
	 * @return
	 */
	public static AppContext getTestUserContext(String defaultLocale,
			String salt, Role role) {
		String email = "test" + System.currentTimeMillis()
				+ "@squidsolutions.com";
		User user = new User(null, email, "Password", email);
		return getTestUserContext(defaultLocale, salt, role, user);
	}

	/**s
	 * Create a test Customer with a give user role.
	 * 
	 * @return
	 */
	public static AppContext getTestUserContext(String defaultLocale,
			String salt, Role role, User user) {
		List<Client> clients = new ArrayList<Client>();
		clients.add(new Client(new ClientPK(null,
				CoreConstants.CONSOLE_CLIENT_ID),
				CoreConstants.CONSOLE_CLIENT_NAME, "" + UUID.randomUUID(), null));
		return applyRole(cs.createCustomer(AUTH_MODE.OAUTH,
				"customer" + System.currentTimeMillis(), defaultLocale, salt,
				"root", "root0000", "root@squidsolutions",clients),
				role, user.getLogin(), user.getPassword(), user.getEmail());
	}

	/**
	 * Create a test Customer with a give user role.
	 * 
	 * @return
	 */
	public static AppContext getTestUserContext(Customer c, Role role) {
		String email = "test" + System.currentTimeMillis()
				+ "@squidsolutions.com";
		List<Client> clients = new ArrayList<Client>();
		clients.add(new Client(new ClientPK(null,
				CoreConstants.CONSOLE_CLIENT_ID),
				CoreConstants.CONSOLE_CLIENT_NAME, "" + UUID.randomUUID(), null));
		return applyRole(cs.createCustomer(c, "root", "root0000",
				"root@squidsolutions", clients), role, email,
				"Password", email);
	}

	private static AppContext applyRole(AppContext ctx, Role role,
			String login, String password, String email) {
		AppContext rootContext = ServiceUtils.getInstance().getRootUserContext(
				ctx.getCustomerId());

		// create a new user
		User newUser = new User(new UserPK(ctx.getCustomerId()), login,
				password, email);
		newUser = DAOFactory.getDAOFactory().getDAO(User.class)
				.create(rootContext, newUser);

		if (role != null) {
			// set the new rights
			Customer customer = DAOFactory.getDAOFactory()
					.getDAO(Customer.class)
					.readNotNull(rootContext, ctx.getCustomerPk());
			AccessRight accessRight = new AccessRight();
			accessRight.setRole(role);
			accessRight.setUserId(newUser.getId().getUserId());
			customer.getAccessRights().add(accessRight);
			DAOFactory.getDAOFactory().getDAO(Customer.class)
					.update(rootContext, customer);
		}

		// create a new AppContext
		AppContext anonymousCtx = new AppContext.Builder(ctx.getCustomerId(),
				ctx.getClientId()).build();
		AccessToken token;
		try {
			AuthServiceImpl authService = AuthServiceImpl.getInstance();
			ClientPK client = new ClientPK(ctx.getCustomerId(),
					ctx.getClientId());
			token = authService.authAndReturnToken(anonymousCtx, client, null,
					login, password);
			return new AppContext.Builder(token, newUser).build();
		} catch (DuplicateUserException e) {
			// should not happen
			throw new RuntimeException(e);
		}
	}
}
