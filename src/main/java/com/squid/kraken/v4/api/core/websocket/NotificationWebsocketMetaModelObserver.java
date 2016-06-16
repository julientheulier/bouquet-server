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

import java.util.Set;

import javax.websocket.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;

/**
 * Observes the Meta-Model.
 */
public class NotificationWebsocketMetaModelObserver implements
		DataStoreEventObserver {

	private static final Logger logger = LoggerFactory
			.getLogger(NotificationWebsocketMetaModelObserver.class);

	private static NotificationWebsocketMetaModelObserver instance;

	public static synchronized NotificationWebsocketMetaModelObserver getInstance() {
		if (instance == null) {
			instance = new NotificationWebsocketMetaModelObserver();
		}
		return instance;
	}

	private NotificationWebsocketMetaModelObserver() {
	}

	@Override
	public void notifyEvent(DataStoreEvent event) {
		logger.debug(event.toString());
		Set<Session> sessions = NotificationWebsocket.getSessions();
		for (Session s : sessions) {
			s.getAsyncRemote().sendText(event.toString());
		}
	}

}
