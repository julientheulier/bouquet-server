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
package com.squid.kraken.v4.api.core.bb;

import com.squid.kraken.v4.model.CustomerPK;

/**
 * @author sergefantino
 *
 */
public class NavigationItem {
	
	private CustomerPK id;
	
	private String name;
	
	private String description;
	
	private String parentRef;
	
	private String selfRef;
	
	private String type;
	
	/**
	 * 
	 */
	public NavigationItem() {
		// TODO Auto-generated constructor stub
	}
	
	public NavigationItem(CustomerPK id, String name, String description, String parentRef, String selfRef, String type) {
		super();
		this.id = id;
		this.name = name!=null?name:"";// avoid having undefined name make everyone happy
		this.description = description;
		this.parentRef = parentRef;
		this.selfRef = selfRef;
		this.type = type;
	}
	
	public NavigationItem(String name, String description, String parentRef, String selfRef, String type) {
		super();
		this.name = name!=null?name:"";// avoid having undefined name make everyone happy
		this.description = description;
		this.parentRef = parentRef;
		this.selfRef = selfRef;
		this.type = type;
	}
	
	public CustomerPK getId() {
		return id;
	}

	public void setId(CustomerPK id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getParentRef() {
		return parentRef;
	}

	public void setParentRef(String parentRef) {
		this.parentRef = parentRef;
	}

	public String getSelfRef() {
		return selfRef;
	}

	public void setSelfRef(String selfRef) {
		this.selfRef = selfRef;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
