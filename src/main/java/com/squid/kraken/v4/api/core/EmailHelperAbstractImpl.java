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

import java.util.List;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public abstract class EmailHelperAbstractImpl implements EmailHelper {

    private String hostName;
    private String sslPort;
    private String senderName;
    private String senderEmail;
    private String senderPassword;

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
     *            *** (must not be null)
     */
    public EmailHelperAbstractImpl(String hostName, String sslPort, String senderName, String senderEmail,
            String senderPassword) {
        if (hostName == null) {
            hostName = "smtp.gmail.com";
        }
        this.hostName = hostName;

        if (sslPort == null) {
            sslPort = "465";
        }
        this.sslPort = sslPort;

        if (senderName == null) {
            senderName = "noreply@squidsolutions.com";
        }
        this.senderName = senderName;

        if (senderEmail == null) {
            senderEmail = "noreply@squidsolutions.com";
        }
        this.senderEmail = senderEmail;

        if (senderPassword == null) {
            throw new RuntimeException("Please set 'mail.senderPassword' system property");
        }
        this.senderPassword = senderPassword;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.squid.kraken.v4.api.core.EmailHelper#sendEmail(java.util.List, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.String)
     */
    public void sendEmail(List<String> dests, List<String> bccAddresses, String subject, String textContent, String htmlContent, String priority)
            throws MessagingException {
        Properties props = new Properties();
        props.put("mail.smtp.host", hostName);
        if (sslPort != null) {
            props.put("mail.smtp.socketFactory.port", sslPort);
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        }
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.port", sslPort);

        Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(senderEmail, senderPassword);
            }
        });

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(senderName));
        for (String dest : dests) {
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(dest));
        }
        if ((bccAddresses != null) && !bccAddresses.isEmpty()) {
            for (String bcc : bccAddresses) {
                message.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(bcc));
            }
        }
        message.setSubject(subject);
        
        if (textContent != null) {
            Multipart mp = new MimeMultipart();
            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setContent(textContent, "text/plain");
            mp.addBodyPart(textPart);
            message.setContent(mp);
        }

        if (htmlContent != null) {
            Multipart mp = new MimeMultipart();
            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(htmlContent, "text/html");
            mp.addBodyPart(htmlPart);
            message.setContent(mp);
        }

        sendMessage(message);
    }
    
    public void sendEmail(List<String> dests, String subject, String textContent, String htmlContent, String priority)
            throws MessagingException {
        sendEmail(dests, null, subject, textContent, htmlContent, priority);
    }

    protected abstract void sendMessage(Message message) throws MessagingException;

}
