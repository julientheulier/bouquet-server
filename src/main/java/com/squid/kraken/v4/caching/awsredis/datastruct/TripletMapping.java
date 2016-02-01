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
package com.squid.kraken.v4.caching.awsredis.datastruct;

public class TripletMapping{
	
	public String DBRef;
	public String ModelRef;
	public String QueryRef;
	
	public TripletMapping(String DBRef, String ModelRef, String QueryRef){
		this.DBRef = DBRef;
		this.ModelRef = ModelRef;
		this.QueryRef = QueryRef;			
	}
	
	public String toString(){
		return DBRef + " " +ModelRef + " " + QueryRef;
		
	}
	
	public boolean equals(Object obj){
		if (! ( obj instanceof TripletMapping))
			return false;
		TripletMapping tm= (TripletMapping) obj;
		if( this.DBRef.equals(tm.DBRef) && this.ModelRef.equals(tm.ModelRef) && this.QueryRef.equals(tm.QueryRef))
			return true;
		else
			return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((DBRef == null) ? 0 : DBRef.hashCode());
		result = prime * result + ((ModelRef == null) ? 0 : ModelRef.hashCode());
		result = prime * result + ((QueryRef == null) ? 0 : QueryRef.hashCode());
		return result;
	}
}
