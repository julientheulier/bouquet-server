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
package com.squid.enterprise.model;

import java.util.Collection;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Return the list of invitations sent
 * @author serge.fantino
 *
 */
@XmlType(namespace = "http://model.enterprise.squid.com")
@XmlRootElement
public class ShareReply {
	
	private Collection<Invitation> invitations;
	
	private Collection<String> errors;
	
	/**
	 * 
	 */
	public ShareReply() {
		// TODO Auto-generated constructor stub
	}

	public ShareReply(Collection<Invitation> invitations) {
		this.invitations = invitations;
	}
	
	/**
	 * @return the invitations
	 */
	public Collection<Invitation> getInvitations() {
		return invitations;
	}
	
	/**
	 * @param invitations the invitations to set
	 */
	public void setInvitations(Collection<Invitation> invitations) {
		this.invitations = invitations;
	}
	
	/**
	 * @return the errors
	 */
	public Collection<String> getErrors() {
		return errors;
	}
	
	/**
	 * @param errors the errors to set
	 */
	public void setErrors(Collection<String> errors) {
		this.errors = errors;
	}

}
