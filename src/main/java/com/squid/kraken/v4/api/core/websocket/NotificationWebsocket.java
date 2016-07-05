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

import com.squid.kraken.v4.api.core.InvalidCredentialsAPIException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.customer.TokenExpiredException;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.runtime.CXFServletService;

@ServerEndpoint(value = "/notification", encoders = { SerializableWebsocketJSONCoder.class })
public class NotificationWebsocket {

	private static final Logger logger = LoggerFactory
			.getLogger(CXFServletService.class);
	private static final Set<Session> sessions = new HashSet<Session>();

	static public Set<Session> getSessions() {
		return Collections.unmodifiableSet(sessions);
	}

	public NotificationWebsocket() {
	}

	@OnOpen
	public void onOpen(Session session) throws IOException {
		// check for an auth token
		String queryString = session.getQueryString();
		if (queryString != null) {
			Map<String, String> splitQuery = splitQuery(queryString);
			String tokenId = splitQuery.get(ServiceUtils.TOKEN_PARAM);
			// create a new context with a new session id
			String bouquetSessionId = UUID.randomUUID().toString();
			try {
				AppContext userContext = ServiceUtils.getInstance()
						.buildUserContext(tokenId, bouquetSessionId);
				// update the session
				session.getUserProperties().put("ctx", userContext);
			} catch (TokenExpiredException e) {
				throw new InvalidCredentialsAPIException(e.getMessage(), false);
			}
			// keep this session
			sessions.add(session);
			logger.debug("Session added with ID : " + session.getId()
					+ " uuid : " + bouquetSessionId);
		} else {
			logger.debug("Session rejected with ID : " + session.getId());
			throw new InvalidCredentialsAPIException("missing auth token",
					false);
		}
	}

	@OnError
	public void onError(Throwable t) {
		t.printStackTrace();
	}

	@OnClose
	public void onClose(Session session) {
		sessions.remove(session);
	}

	@OnMessage
	public void onMessage(Session session, String msg, boolean last) {
		try {
			if (session.isOpen()) {
				// send back the welcome message
				try {
					AppContext userContext = (AppContext) session
							.getUserProperties().get("ctx");
					String bouquetSessionId = userContext.getSessionId();
					logger.debug("Welcome session : " + session.getId()
							+ " uuid : " + bouquetSessionId);
					session.getBasicRemote().sendObject(
							new WelcomeMessage(bouquetSessionId));
				} catch (EncodeException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			try {
				session.close();
			} catch (IOException e1) {
				// Ignore
			}
		}
	}

	public Map<String, String> splitQuery(String query)
			throws UnsupportedEncodingException {
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
	public static class WelcomeMessage implements Serializable {
		private final String bouquetSessionId;

		public WelcomeMessage(String bouquetSessionId) {
			super();
			this.bouquetSessionId = bouquetSessionId;
		}

		public String getBouquetSessionId() {
			return bouquetSessionId;
		}
	}

}