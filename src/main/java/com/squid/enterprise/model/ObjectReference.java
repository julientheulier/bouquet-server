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

import java.net.URL;

/**
 * @author serge.fantino
 *
 */
public class ObjectReference {
	
	public class Binding <T> {
		
		private T object;
		
		public Binding(T object) {
			this.object = object;
		}
		
		/**
		 * @return the object
		 */
		public T getObject() {
			return object;
		}
		
		public ObjectReference getObjectReference() {
			return ObjectReference.this;
		}
	}

	// this the the new resource identifier, based on /analytics API
	private String reference = null;

	// this is a link to open the resource in the app that created the snippet
	private URL applicationURL = null;
	
	/**
	 * 
	 */
	public ObjectReference() {
		// TODO Auto-generated constructor stub
	}
	
	public ObjectReference(String reference) {
		this.reference = reference;
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public URL getApplicationURL() {
		return applicationURL;
	}

	public void setApplicationURL(URL applicationURL) {
		this.applicationURL = applicationURL;
	}
	
	public <T> Binding<T> bind(T object) {
		return new Binding<T>(object);
	}

	@Override
	public String toString() {
		return "ObjectReference [reference=" + reference + ", applicationURL=" + applicationURL + "]";
	}

}
