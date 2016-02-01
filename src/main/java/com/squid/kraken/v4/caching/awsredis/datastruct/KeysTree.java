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

public class KeysTree {

	private String label;
	private int key ;
	private HashMap<String, KeysTree> descendants ;

	
	public KeysTree (String name, KeysTree ancestor ){
		this.label = name;
		this.descendants = new HashMap<String, KeysTree>();
		this.key= 0;
	}
	
	public int getKey(){
		return this.key;
	}
	
	private synchronized void incKey(){
		this.key = this.key +1;
	}	
	
	
	public static KeysTree getRootTree(){
		return new KeysTree("root", null);
	}
	
	
	//GET NODE
	public KeysTree getNode(String ID){
		if(descendants.containsKey(ID))
			return this.descendants.get(ID);
		else{
			KeysTree res=  null;
			for(KeysTree t : this.descendants.values()){
				res = t.getNode(ID);
				if (res != null)
					break;
			}
			return res;
		}		
	}
			
	
	
	
	///GET KEYS

	// fuse trees and get key
	public HashMap <String, Integer> fuseAndGetKey(KeysTree dependencies){
		HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
		
		if (this.label !="root" || dependencies.label !="root")
			return null; 
		
		// fuse
		for (String  dep : dependencies.descendants.keySet() )
			if (this.descendants.containsKey(dep)){
				KeysTree toFuse = this.descendants.get(dep);				
				toFuse.fuseTree(dependencies.descendants.get(dep));
			}else{
				this.addSubTree(dependencies.descendants.get(dep));
			}
		//get generational keys, skip root level
		for (String  dep : dependencies.descendants.keySet() )
			keyMap.putAll( this.descendants.get(dep).getKeys(dependencies.descendants.get(dep)) );

		return keyMap;
	}

	private void addSubTree(KeysTree target){
		this.descendants.put(target.label, target);
	}
	
	private void fuseTree(KeysTree target){
		for (String dep : target.descendants.keySet())
			if (this.descendants.containsKey(dep))
				this.descendants.get(dep).fuseTree(target.descendants.get(dep));
			else
				this.addSubTree(target.descendants.get(dep));
	}
	
	private HashMap<String, Integer> getKeys(KeysTree dependencies){
		HashMap<String, Integer> res  = new HashMap<String, Integer>();
		res.put(this.label, this.getKey());
		for (String dep : dependencies.descendants.keySet())
			res.putAll( this.descendants.get(dep).getKeys( dependencies.descendants.get(dep) ) );
		return res;
	}
	
	// INVALIDATION - UPDATE KEYS
	
	public void update(KeysTree dependencies){
		this.incKey();
		for (String dep : dependencies.descendants.keySet())
			if (this.descendants.containsKey(dep))
				this.descendants.get(dep).update(dependencies.descendants.get(dep));
	} 
	
	public boolean invalidate(HashSet<String> dependencies){
		
		boolean isInvalidated = false;
		if (dependencies.contains(this.label)){
			isInvalidated = true;
			dependencies.remove(this.label);
		}
		if (dependencies.isEmpty()){
			if (isInvalidated)
				this.incKey();
			return isInvalidated; 
		}
		else{
			boolean invalidatedChild = false;
			for(KeysTree k : this.descendants.values())
				invalidatedChild = invalidatedChild || k.invalidate(dependencies);
			if (invalidatedChild)
				this.incKey();
			return invalidatedChild || isInvalidated; 
		}		
	}
	
	// BUILDING
	
	public KeysTree addNode(ArrayList<String>  path){
		if (path.isEmpty())
			return this ;
		else{
			String name = path.remove(0);
			KeysTree next  = this.descendants.get(name) ;
			if ( next == null){
				next= new KeysTree(name, this);
				this.descendants.put(name, next);
			}
			return next.addNode(path);	
		}
	}

	
	//DISPLAY
	
	public String prettyPrint(String prefix){
		String res = prefix + label + " " + key +"\n" ;
		for (KeysTree k : this.descendants.values())
			res  += k.prettyPrint(prefix+"\t");
		return res;
	}
}

