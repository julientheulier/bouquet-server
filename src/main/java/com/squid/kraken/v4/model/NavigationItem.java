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
package com.squid.kraken.v4.model;

import java.net.URI;
import java.util.Map;

import com.squid.kraken.v4.model.NavigationQuery.Style;

/**
 * @author sergefantino
 *
 */
public class NavigationItem {

	public static final String PROJECT_TYPE = "PROJECT";
	public static final String FOLDER_TYPE = "FOLDER";
	public static final String BOOKMARK_TYPE = "BOOKMARK";
	public static final String DOMAIN_TYPE = "DOMAIN";
	
	private CustomerPK id;
	
	private String name;
	
	private String description;
	
	private Map<String, String> attributes;
	
	private String parentRef;
	
	private String selfRef;
	
	private String type;
	
	private URI link;
	
	private URI upLink;// link to parent
	
	private URI objectLink;// link to the actual object
	
	private URI viewLink;// link to the /view API
	
	private Object objectID;//
	
	/**
	 * 
	 */
	public NavigationItem() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * wrap a project
	 * @param query
	 * @param project
	 * @param parentRef
	 */
	public NavigationItem(NavigationQuery query, Project project, String parentRef) {
		this.id = query.getStyle()==Style.LEGACY?project.getId():null;
		this.name = project.getName();
		if (this.name==null) this.name="";
		this.description = project.getDescription();
		this.parentRef = parentRef;
		if (query.getStyle()==Style.HUMAN || query.getStyle()==Style.HTML) {
			this.selfRef = parentRef+"/"+project.getName();
		} else {
			this.selfRef = parentRef+"/@"+project.getOid();
		}
		this.type = PROJECT_TYPE;
		this.objectID = project.getId();
	}
	
	/**
	 * wrap a domain
	 * @param query
	 * @param project
	 * @param domain
	 * @param parent
	 */
	public NavigationItem(NavigationQuery query, Project project, Domain domain, String parentRef) {
		this.id = query.getStyle()==Style.LEGACY?domain.getId():null;
		this.name = domain.getName();
		if (this.name==null) this.name="";
		this.description = domain.getDescription();
		this.parentRef = parentRef;
		if (query.getStyle()==Style.HUMAN) {
			this.selfRef = "'"+project.getName()+"'.'"+domain.getName()+"'";
		} else {
			this.selfRef = "@'"+project.getOid()+"'.@'"+domain.getOid()+"'";
		}
		this.type = DOMAIN_TYPE;
		this.objectID = domain.getId();
	}
	
	/**
	 * wrap a bookmark
	 * @param query
	 * @param project
	 * @param bookmark
	 * @param parentRef
	 */
	public NavigationItem(NavigationQuery query, Project project, Bookmark bookmark, String parentRef) {
		this.id = query.getStyle()==Style.LEGACY?bookmark.getId():null;
		this.name = bookmark.getName();
		if (this.name==null) this.name="";
		this.description = bookmark.getDescription();
		this.parentRef = parentRef;
		if (query.getStyle()==Style.HUMAN) {
			// only use the project name
			// cannot rely on the bookmark name for lookup
			this.selfRef =  "'"+project.getName()+"'.@bookmark:'"+bookmark.getOid()+"'";
		} else {
			this.selfRef =  "@'"+project.getOid()+"'.@bookmark:'"+bookmark.getOid()+"'";
		}
		this.type = BOOKMARK_TYPE;
		this.objectID = bookmark.getId();
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
	
	public NavigationItem(NavigationItem copy) {
		super();
		this.id = copy.id;
		this.name = copy.name;// avoid having undefined name make everyone happy
		this.description = copy.description;
		this.parentRef = copy.parentRef;
		this.selfRef = copy.selfRef;
		this.type = copy.type;
		this.objectID = copy.objectID;
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
	
	public Map<String, String> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<String, String> attributes) {
		this.attributes = attributes;
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

	public URI getLink() {
		return link;
	}

	public void setLink(URI link) {
		this.link = link;
	}

	public URI getUpLink() {
		return upLink;
	}

	public void setUpLink(URI upLink) {
		this.upLink = upLink;
	}

	public URI getObjectLink() {
		return objectLink;
	}
	
	public Object getObjectID() {
		return objectID;
	}
	
	public void setObjectID(Object objectID) {
		this.objectID = objectID;
	}

	public void setObjectLink(URI objectLink) {
		this.objectLink = objectLink;
	}

	public URI getViewLink() {
		return viewLink;
	}

	public void setViewLink(URI viewLink) {
		this.viewLink = viewLink;
	}

}
