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
package com.squid.kraken.v4.caching.awsredis.generationalkeysserver;


import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class RedisKey {

	private String name;
	private HashMap<String, Integer> depGen;	
	private UUID uniqueID;
	private int version;
	private Date lastUpdate;
	
	static final Logger logger = LoggerFactory
			.getLogger(RedisKey.class);
	
	
	public RedisKey(){
	}
	
	public RedisKey(String name, UUID uniqueID, int version,  HashMap<String, Integer> depGen){		
		this.name = name ;
		this.uniqueID = uniqueID;
		if( depGen == null)
			this.depGen = new HashMap<String, Integer>();
		else
			this.depGen = depGen;
		this.version = version;
		this.lastUpdate = new Date();
	}
	
	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public void increaseVersion(){
		this.version +=1;
		this.lastUpdate = new Date();
	}
	
	public Date getLastUpdate() {
		return lastUpdate;
	}
	
	public void setLastUpdate(Date versionDate) {
		this.lastUpdate = versionDate;
	}

	public String getName() {
		return name;
	}
	public UUID getUniqueID() {
		return uniqueID;
	}
	public HashMap<String, Integer> getDepGen() {
		return depGen;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public void setDepGen(HashMap<String, Integer> depGen) {
		this.depGen = depGen;
	}

	public void setUniqueID(UUID uniqueID) {
		this.uniqueID = uniqueID;
	}
	
	@JsonIgnore
	public boolean isTerminalDependency(){
		return ( this.depGen == null || this.depGen.isEmpty());
	}
	
	public String toJson() throws JsonProcessingException{
		ObjectMapper mapper = new ObjectMapper();
	    String res =  mapper.enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(this);
	    return res;
	}
	
	public String toString() {
		String res ="" ;
		res += this.name + " ";
		res+=this.uniqueID.toString() + " "; 
		res+= this.version;
		if (this.depGen!=null) {
	        res += " " + this.depGen.toString();
		}
		return res; 
	}
	
	public static RedisKey fromJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
			return mapper.readValue(json, RedisKey.class);
		} catch (IOException e) {
			logger.info(e.getMessage());
			return null;
		}
	}
	
	/**
	 * return UUID+version
	 * @return
	 */
	@JsonIgnore
	public String getStringKey(){
		return this.uniqueID.toString() + "-"+this.version;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((depGen == null) ? 0 : depGen.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((uniqueID == null) ? 0 : uniqueID.hashCode());
		result = prime * result + version;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RedisKey other = (RedisKey) obj;
		if (depGen == null) {
			if (other.depGen != null)
				return false;
		} else if (!depGen.equals(other.depGen))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (uniqueID == null) {
			if (other.uniqueID != null)
				return false;
		} else if (!uniqueID.equals(other.uniqueID))
			return false;
		if (version != other.version)
			return false;
		return true;
	}

    
}
