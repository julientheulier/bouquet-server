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

import java.util.List;

/**
 * 
 * An invitation is targettd as one user for one or many resources, described by Snippets
 * @author serge.fantino
 *
 */
public class Invitation {
	
	private UserAcessLevel recipient;
	
	private List<Snippet> snippets;
	
	/**
	 * 
	 */
	public Invitation() {
		// TODO Auto-generated constructor stub
	}

	public Invitation(UserAcessLevel recipient, List<Snippet> snippets) {
		super();
		this.recipient = recipient;
		this.snippets = snippets;
	}

	public UserAcessLevel getRecipient() {
		return recipient;
	}

	public void setRecipient(UserAcessLevel recipient) {
		this.recipient = recipient;
	}

	public List<Snippet> getSnippets() {
		return snippets;
	}

	public void setSnippets(List<Snippet> snippets) {
		this.snippets = snippets;
	}

}
