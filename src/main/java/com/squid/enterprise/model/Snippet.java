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

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A snippet describe how to access a resource on the server and its content, so that remote OB.io can "show" it users
 * @author serge.fantino
 *
 */
@XmlType(namespace = "http://model.enterprise.squid.com")
@XmlRootElement
public class Snippet extends ObjectReference {
	
	// this is the resource type
	private String type = null;
	
	// this is a resource name
	private String name = null;
	
	// this is the resource path relative to the /analytics API
	private String path = null;
	
	// this is the resource description if provided
	private String description = null;
	
	/**
	 * 
	 */
	public Snippet() {
		// TODO Auto-generated constructor stub
	}
	
	public Snippet(ObjectReference reference) {
		super(reference.getReference());
		setApplicationURL(reference.getApplicationURL());
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return "Snippet [type=" + type + ", name=" + name + ", path=" + path + ", description=" + description
				+ ", " + super.toString() + "]";
	}

}
