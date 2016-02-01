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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Invalidation {

	
	HashMap<String, ArrayList<String>> Model2Query ;
	HashMap<String, ArrayList<String>> DB2Model ;
	
	KeysTree QueryTree ;
	
	public Invalidation(){
		
	}
	
	public void addConnection(TripletMapping t)
	{
		ArrayList<String> DBdeps =  DB2Model.get(t.DBRef);
		if ( DBdeps!=null){
			if (! DBdeps.contains (t.ModelRef))
				DBdeps.add(t.ModelRef);
		}else{
			DBdeps = new ArrayList<String>();
			DBdeps.add(t.ModelRef);
			this.DB2Model.put(t.DBRef, DBdeps);
		}

		ArrayList<String> Modeldeps =  Model2Query.get(t.ModelRef);
		if ( Modeldeps!=null){
			if (! Modeldeps.contains (t.QueryRef))
				Modeldeps.add(t.QueryRef);
		}else{
			Modeldeps = new ArrayList<String>();
			Modeldeps.add(t.QueryRef);
			this.Model2Query.put(t.ModelRef, Modeldeps);
		}
	}
	
	public void invalidate(String DBRef){
		ArrayList<String> modelRefs  = this.DB2Model.get(DBRef);
		
		HashSet<String> queryRefs = new HashSet<String>();
		for(String modelRef : modelRefs){
			queryRefs.addAll(this.Model2Query.get(modelRef));
		}
		this.QueryTree.invalidate(queryRefs);
	}
	
	
}
