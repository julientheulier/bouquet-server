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

import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlType;

/**
 * A BookmarkFolder is specific object used to navigate through folders defined in Bookmarks and
 * regardless of Projects.
 */
@XmlType(namespace = "http://model.v4.kraken.squid.com")
public class BookmarkFolder implements HasChildren {

	private static String[] CHILDREN = { "folders" };

	private BookmarkFolderPK id;
	private String name;
	private List<BookmarkLink> bookmarks;

	/**
	 * Default constructor (required for jaxb).
	 */
	public BookmarkFolder() {
	}
	
	public BookmarkFolder(BookmarkFolderPK id) {
		this.id = id;
	}

	public BookmarkFolderPK getId() {
		return id;
	}

	public void setId(BookmarkFolderPK id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<BookmarkLink> getBookmarks() {
		return bookmarks;
	}

	public void setBookmarks(List<BookmarkLink> bookmarks) {
		this.bookmarks = bookmarks;
	}

	@Override
	public String[] getChildren() {
		return CHILDREN;
	}
	
	/*
	 * Stupid getter for Backbone nested model.
	 * @return empty list
	 */
	public List<BookmarkFolder> getFolders() {
		return Collections.<BookmarkFolder> emptyList();
	}
	
	/**
	 * Bookmark holds a project configuration
	 */
	static public class BookmarkLink {
		
		private BookmarkPK id;

		private String name;
		private String description;

		/**
		 * Default constructor (required for jaxb).
		 */
		public BookmarkLink() {
		}

		public BookmarkLink(BookmarkPK id) {
			this.id = id;
		}

		public BookmarkPK getId() {
			return id;
		}

		public void setId(BookmarkPK id) {
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

	}


}
