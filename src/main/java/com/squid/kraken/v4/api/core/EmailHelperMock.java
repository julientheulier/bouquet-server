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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;

public class EmailHelperMock extends EmailHelperAbstractImpl {
    
    public Queue<Message> messageQueue;

    public EmailHelperMock() {
        super(null, null, null, null, "senderPassword");
        messageQueue = new ArrayBlockingQueue<Message>(100);
    }
    
    protected void sendMessage(Message message) throws MessagingException {
        messageQueue.add(message);
    }
    
    /**
     * Utility method to read message body
     * @param content object from message.getContent()
     * @return content body as string
     */
    public String getBody(Object content) {
        String body = null;
        if (content instanceof String) {
            body = (String) content;
        }
        try {
            if (content instanceof Multipart) {
                Multipart mp = (Multipart) content;

                body = getBody(mp.getBodyPart(0));

            }
            if (content instanceof BodyPart) {
                BodyPart p = (BodyPart) content;
                body = getBody(p.getContent());
            }
        } catch (Exception e) {
            e.printStackTrace();
            body = null;
        }
        return body;
    }

}
