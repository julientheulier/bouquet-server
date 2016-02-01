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

import javax.xml.bind.annotation.XmlRootElement;

@SuppressWarnings("serial")
@XmlRootElement
public class BookmarkPK extends ProjectPK {

	private String bookmarkId;

	public BookmarkPK() {
	}

	public BookmarkPK(ProjectPK parent) {
		this(parent.getCustomerId(), parent.getProjectId(), null);
	}
	
	public BookmarkPK(BookmarkPK id) {
		this(id.getCustomerId(), id.getProjectId(), id.getBookmarkId());
	}

	public BookmarkPK(String customerId, String projectId) {
		this(customerId, projectId, null);
	}

	public BookmarkPK(ProjectPK parent, String bookmarkId) {
		this(parent.getCustomerId(), parent.getProjectId(), bookmarkId);
	}

	public BookmarkPK(String customerId, String projectId, String bookmarkId) {
		super(customerId, projectId);
		this.bookmarkId = bookmarkId;
	}

	public String getBookmarkId() {
		return bookmarkId;
	}

	public void setBookmarkId(String bookmarkId) {
		this.bookmarkId = bookmarkId;
	}

	@Override
	public String getObjectId() {
		return bookmarkId;
	}

	@Override
	public void setObjectId(String id) {
		bookmarkId = id;
	}

	@Override
	public String toString() {
		return "BookmarkPK [bookmarkId=" + bookmarkId + ", getProjectId()="
				+ getProjectId() + "]";
	}

	@Override
	public ProjectPK getParent() {
		return new ProjectPK(getCustomerId(), getProjectId());
	}

	@Override
	public void setParent(GenericPK pk) {
		setCustomerId(((ProjectPK) pk).getCustomerId());
		setProjectId(((ProjectPK) pk).getProjectId());
	}
}
