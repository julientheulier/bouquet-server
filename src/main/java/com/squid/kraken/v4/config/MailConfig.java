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
package com.squid.kraken.v4.config;


public  class MailConfig{
	
	@Override
	public String toString() {
		return "MailConfig [senderPassword=" + senderPassword + ", hostname="
				+ hostname + ", senderName=" + senderName + ", senderEmail="
				+ senderEmail + ", sslPort=" + sslPort + "]";
	}
	public MailConfig(String senderPassword, String hostname,
			String senderName, String senderEmail, int sslPort) {
		super();
		this.senderPassword = senderPassword;
		this.hostname = hostname;
		this.senderName = senderName;
		this.senderEmail = senderEmail;
		this.sslPort = sslPort;
	}
	public MailConfig(){	
	}
	public String getSenderPassword() {
		return senderPassword;
	}
	public void setSenderPassword(String senderPassword) {
		this.senderPassword = senderPassword;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public String getSenderName() {
		return senderName;
	}
	public void setSenderName(String senderName) {
		this.senderName = senderName;
	}
	public String getSenderEmail() {
		return senderEmail;
	}
	public void setSenderEmail(String senderEmail) {
		this.senderEmail = senderEmail;
	}
	public int getSslPort() {
		return sslPort;
	}
	public void setSslPort(int sslPort) {
		this.sslPort = sslPort;
	}
	String senderPassword;
	String hostname;
	String senderName;
	String senderEmail;
	int sslPort	;
}