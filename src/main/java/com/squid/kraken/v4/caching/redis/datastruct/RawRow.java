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
package com.squid.kraken.v4.caching.redis.datastruct;

import java.io.Serializable;

/**
 * a super simple row of objects
 * 
 * @author sergefantino
 *
 */
public class RawRow implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5298105092629830699L;
	protected Object[] data;

	public RawRow(int size) {
		this.data = new Object[size];
	}

	public RawRow(Object[] objs) {
		this.data = objs;
	}

	public Object[] getData() {
		return this.data;
	}
	
	public Object getData(int i) {
		return this.data[i];
	}
	
	public void setData(int i, Object value) {
		this.data[i] = value;
	}

	public int size() {
		return data != null ? data.length : 0;
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RawRow)) {
			return false;
		}
		RawRow r = (RawRow) obj;
		if (this.data.length != r.data.length)
			return false;
		for (int i = 0; i < this.data.length; i++) {
			if (!this.data[i].equals(r.data[i])) {
				return false;
			}
		}
		return true;
	}

	public String toString() {
		String res = "";
		for (Object o : data) {
			if (o != null) {
				res += o.toString() + "\t";
			} else {
				res += "None ";
			}
		}
		return res;
	}

}
