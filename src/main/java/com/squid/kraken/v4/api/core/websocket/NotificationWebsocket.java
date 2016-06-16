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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.PongMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.runtime.CXFServletService;

@ServerEndpoint(value="/notification")
public class NotificationWebsocket {
	
	private static final Logger logger = LoggerFactory.getLogger(CXFServletService.class);
	private static final Set<Session> sessions = new HashSet<Session>();
	
	public NotificationWebsocket() {
	}
	
	static public Set<Session> getSessions() {
		return sessions;
	}

	@OnOpen
    public void onOpen(Session session) throws IOException {
		sessions.add(session);
		logger.debug("Session added with ID : "+session.getId());
        session.getBasicRemote().sendText("onOpen");
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
    public void echoTextMessage(Session session, String msg, boolean last) {
        try {
            if (session.isOpen()) {
            	logger.debug("Message received from Session with ID : "+session.getId());
                session.getBasicRemote().sendText(msg, last);
            }
        } catch (IOException e) {
            try {
                session.close();
            } catch (IOException e1) {
                // Ignore
            }
        }
    }

    @OnMessage
    public void echoBinaryMessage(Session session, ByteBuffer bb,
            boolean last) {
        try {
            if (session.isOpen()) {
                session.getBasicRemote().sendBinary(bb, last);
            }
        } catch (IOException e) {
            try {
                session.close();
            } catch (IOException e1) {
                // Ignore
            }
        }
    }

    /**
     * Process a received pong. This is a NO-OP.
     *
     * @param pm    Ignored.
     */
    @OnMessage
    public void echoPongMessage(PongMessage pm) {
        // NO-OP
    }
}