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

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * define what/who to share
 * @author serge.fantino
 *
 */
@XmlType(namespace = "http://model.enterprise.squid.com")
@XmlRootElement
public class ShareQuery {
	
	private List<ObjectReference> resources = null;
	
	private List<UserAcessLevel> sharing = null;
	
	/**
	 * 
	 */
	public ShareQuery() {
		// TODO Auto-generated constructor stub
	}

	public List<ObjectReference> getResources() {
		return resources;
	}

	public void setResources(List<ObjectReference> resources) {
		this.resources = resources;
	}

	public List<UserAcessLevel> getSharing() {
		return sharing;
	}

	public void setSharing(List<UserAcessLevel> sharing) {
		this.sharing = sharing;
	}

}
