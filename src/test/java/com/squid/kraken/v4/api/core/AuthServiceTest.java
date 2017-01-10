package com.squid.kraken.v4.api.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import com.squid.kraken.v4.api.core.test.BaseTest;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.AccessTokenPK;
import com.squid.kraken.v4.model.AuthCode;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

public class AuthServiceTest extends BaseTest {
	
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
	 * @param authorizationCode
	 * @return an AccessToken
	 */
	static public AccessToken createToken(AppContext ctx, ClientPK clientPk,
			String userId, Long creationTimestamp, Long validityMillis, String authorizationCode) {
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

	@Test
	public void testLoginServerSide2() {
		// Given a valid User with a valid auth code
		AppContext ctx = CustomerServiceTest.getTestUserContext(null, null,
				Role.OWNER);
		User newUser = addUser(ctx.getCustomerId());
		Client client = clients.store(ctx,
				new Client(new ClientPK(ctx.getCustomerId()), "test", "secret",
						Arrays.asList("www.example.com")));
		String redirectURI = "http://www.example.com/app";
		AuthCode code = aus.authAndReturnCode(ctx, client.getId(), redirectURI,
				newUser.getLogin(), newUser.getPassword(), false);
		System.out.println("AuthCode : " + code.getCode());

		// When he exchanges his auth code for an access token twice
		InvalidCredentialsAPIException ex = null;
		AccessToken findToken = aus.getTokenFromAuthCode(ctx, client.getId(),
				redirectURI, code.getCode());

		try {
			AccessToken findToken2 = aus.getTokenFromAuthCode(ctx,
					client.getId(), redirectURI, code.getCode());
		} catch (InvalidCredentialsAPIException e) {
			ex = e;
		}

		// Then an InvalidCredentialsAPIException is thrown
		assertTrue(ex != null);
	}

	@Test
	public void testLogout() {
		// Given 2 valid tokens for the same user
		AppContext rootctx = CustomerServiceTest.createTestUserContext();
		Long time = System.currentTimeMillis();
		Long validity = 1000l;
		AccessToken validToken = ServiceUtils.getInstance().createToken(rootctx.getCustomerId(), null, rootctx
				.getUser().getId().getUserId(), time, validity, null, null);
		AccessToken validToken2 = ServiceUtils.getInstance().createToken(rootctx.getCustomerId(), null, rootctx
				.getUser().getId().getUserId(), time, validity, null, null);

		// When logout using this token
		aus.logoutUser(rootctx);

		// Then both tokens are invalidated
		try {
			AccessToken t = ServiceUtils.getInstance().getToken(
					validToken.getId().getTokenId());
			assertNull(t);
		} catch (Exception e) {
			fail();
		}

		try {
			AccessToken t = ServiceUtils.getInstance().getToken(
					validToken2.getId().getTokenId());
			assertNull(t);
		} catch (Exception e) {
			fail();
		}
	}
	
	@Test
	public void testToken() {
		// Given a test customer
		AppContext rootctx = CustomerServiceTest.createTestUserContext();
		String customerId = rootctx.getCustomerId();

		// When creating a valid token
		Long time = System.currentTimeMillis();
		Long validity = 1000000l;
		AccessToken validToken = AuthServiceTest.createToken(rootctx, null, rootctx
				.getUser().getId().getUserId(), time, validity, null);

		// Then the token shall have the test customer customer id
		assertEquals(customerId, validToken.getCustomerId());

		// Then the token expiration date should be (creation date + validity)
		assertEquals(new Long(time + validity),
				validToken.getExpirationDateMillis());

		// Then retrieving the token should not raise a TokenExpiredException
		try {
			AccessToken t = ServiceUtils.getInstance().getToken(
					validToken.getId().getTokenId());
			assertEquals(customerId, t.getCustomerId());
		} catch (Exception e) {
			fail();
		}

		// Then retrieving an inexisting token should not raise a
		// TokenExpiredException and return null
		try {
			AccessToken t = ServiceUtils.getInstance().getToken("fake");
			assertEquals(t, null);
		} catch (Exception e) {
			fail();
		}

		// When creating a expired token
		AccessToken expiredToken = AuthServiceTest.createToken(rootctx, null, rootctx
				.getUser().getId().getUserId(), time, 0l, null);

		// Then retrieving an expired token should raise a TokenExpiredException
		try {
			ServiceUtils.getInstance().getToken(
					expiredToken.getId().getTokenId());
			fail();
		} catch (Exception e) {
			// OK
		}
	}

}
