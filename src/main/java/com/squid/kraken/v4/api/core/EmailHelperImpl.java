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

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;

import com.squid.kraken.v4.KrakenConfig;

public class EmailHelperImpl extends EmailHelperAbstractImpl {

    static private EmailHelper instance;

    static public EmailHelper getInstance() {
        if (instance == null) {
            String hostName = KrakenConfig.getProperty("mail.hostName");
            String sslPort = KrakenConfig.getProperty("mail.sslPort");
            String senderName = KrakenConfig.getProperty("mail.senderName");
            String senderEmail = KrakenConfig.getProperty("mail.senderEmail");
            String senderPassword = KrakenConfig.getProperty("mail.senderPassword");
            instance = new EmailHelperImpl(hostName, sslPort, senderName, senderEmail, senderPassword);
        }
        return instance;
    }

    /**
     * @param hostName
     *            smtp.gmail.com
     * @param sslPort
     *            465
     * @param senderName
     *            squid
     * @param senderEmail
     *            ###@squidsolutions.com
     * @param senderPassword
     *            ***
     */
    public EmailHelperImpl(String hostName, String sslPort, String senderName, String senderEmail, String senderPassword) {
        super(hostName, sslPort, senderName, senderEmail, senderPassword);

    }

    protected void sendMessage(Message message) throws MessagingException {
        Transport.send(message);
    }

}
