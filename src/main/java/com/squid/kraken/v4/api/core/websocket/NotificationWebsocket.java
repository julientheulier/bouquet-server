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

package com.squid.kraken.v4.api.core.websocket;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.websocket.EncodeException;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.customer.TokenExpiredException;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;

@ServerEndpoint(value = "/notification", encoders = { SerializableWebsocketJSONCoder.class })
public class NotificationWebsocket {

	private static final Logger logger = LoggerFactory.getLogger(NotificationWebsocket.class);
	private static final Set<Session> sessions = Collections.synchronizedSet(new HashSet<Session>());
	private static final SetMultimap<String, Session> sessionsByToken = HashMultimap.create();

	static public Set<Session> getSessions() {
		return Collections.unmodifiableSet(sessions);
	}

	static public Set<Session> getSessionsByToken(String tokenId) {
		return sessionsByToken.get(tokenId);
	}

	public NotificationWebsocket() {
	}

	@OnOpen
	public void onOpen(Session session) throws IOException, EncodeException {
		// check for an auth token
		String tokenId = null;
		String queryString = session.getQueryString();
		if (queryString != null) {
			Map<String, String> splitQuery = splitQuery(queryString);
			tokenId = splitQuery.get(ServiceUtils.TOKEN_PARAM);
		}
		// create a new context with a new session id
		String bouquetSessionId = UUID.randomUUID().toString();
		try {
			AppContext userContext = buildUserContext(tokenId, bouquetSessionId);
			// update the session
			session.getUserProperties().put("ctx", userContext);
			// keep this session
			sessions.add(session);
			Multimaps.synchronizedSetMultimap(sessionsByToken).put(tokenId, session);
			logger.info("Session added with ID : " + session.getId() + " uuid : " + bouquetSessionId);
		} catch (TokenExpiredException | InvalidCredentialsAPIException e) {
			// send a logout message
			logger.info("Invalid or expired token : " + tokenId);
			session.getBasicRemote().sendObject(new SessionMessage(bouquetSessionId, true, true));
		}
	}

	@OnError
	public void onError(Throwable t) {
		logger.info(t.getMessage(), t);
	}

	@OnClose
	public void onClose(Session session) {
		sessions.remove(session);
		AppContext userContext = (AppContext) session.getUserProperties().get("ctx");
		if ((userContext != null) && (userContext.getToken() != null)) {
			sessionsByToken.remove(userContext.getToken().getOid(), session);
		}
	}

	@OnMessage
	public void onMessage(Session session, String msg, boolean last) throws EncodeException {
		try {
			if (session.isOpen()) {
				// send back the welcome message
				AppContext userContext = (AppContext) session.getUserProperties().get("ctx");
				String bouquetSessionId = userContext.getSessionId();
				logger.debug("Welcome session : " + session.getId() + " uuid : " + bouquetSessionId);
				session.getBasicRemote().sendObject(new SessionMessage(bouquetSessionId));
			}
		} catch (IOException e) {
			try {
				session.close();
			} catch (IOException e1) {
				// Ignore
			}
		}
	}

	public Map<String, String> splitQuery(String query) throws UnsupportedEncodingException {
		Map<String, String> query_pairs = new LinkedHashMap<String, String>();
		String[] pairs = query.split("&");
		for (String pair : pairs) {
			int idx = pair.indexOf("=");
			query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"),
					URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
		}
		return query_pairs;
	}

	@SuppressWarnings("serial")
	public static class SessionMessage implements Serializable {
		private final String bouquetSessionId;
		private final boolean logout;
		private final boolean expired;

		public SessionMessage(String bouquetSessionId) {
			this(bouquetSessionId, false, false);
		}

		public SessionMessage(String bouquetSessionId, boolean logout, boolean expired) {
			super();
			this.bouquetSessionId = bouquetSessionId;
			this.logout = logout;
			this.expired = expired;
		}

		public String getBouquetSessionId() {
			return bouquetSessionId;
		}

		public boolean isLogout() {
			return logout;
		}

		public boolean isExpired() {
			return expired;
		}
	}

	public static AppContext buildUserContext(String tokenId, String sessionId) throws TokenExpiredException {
		AccessToken token = null;
		AppContext ctx = null;

		// retrieve the token
		token = ServiceUtils.getInstance().getToken(tokenId);
		if (token == null) {
			throw new InvalidCredentialsAPIException("Invalid token", false);
		} else {
			// retrieve the User
			AppContext root = ServiceUtils.getInstance().getRootUserContext(token.getCustomerId());
			User user = DAOFactory.getDAOFactory().getDAO(User.class).readNotNull(root,
					new UserPK(token.getCustomerId(), token.getUserId()));

			// build the context
			AppContext.Builder ctxb = new AppContext.Builder(token, user);
			ctxb.setSessionId(sessionId);
			ctx = ctxb.build();
			return ctx;
		}
	}

}