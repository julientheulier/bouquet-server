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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RedisCacheValuesList extends RedisCacheValue {

	
	public enum State{
		ONGOING,  DONE, ERROR

	}
	
	private  ArrayList<ChunkRef> referenceKeys ;
	private State state;


	public RedisCacheValuesList(){
		this.referenceKeys  = new ArrayList<ChunkRef>();
		this.state = State.ONGOING;
	}

	

	public RedisCacheValuesList(ArrayList<ChunkRef>  refs){
		this();
		this.referenceKeys = refs;
	}


	public void addReferenceKey(ChunkRef key){
		this.referenceKeys.add(key);
	}

	public ArrayList<ChunkRef>  getReferenceKeys() {
		return referenceKeys;
	}

	public void setReferenceKey(ArrayList<ChunkRef>  referenceKeys) {
		this.referenceKeys = referenceKeys;
	}


	public void setDone(){
		this.state=State.DONE;
	}

	public void setError(){
		this.state = State.ERROR;
	}
	
	public boolean isDone(){
		return this.state == State.DONE;
	} 

	public boolean isError(){
		return this.state == State.ERROR;	
	}
	
	public boolean isOngoing(){
		return this.state == State.ONGOING;
	}
	public State getState(){
		return this.state;
	}
	
	
	@Override
	public boolean equals(Object obj){
		if (obj instanceof RedisCacheValuesList ){
			RedisCacheValuesList o = (RedisCacheValuesList) obj;
			if (this.state != o.state){
				return false;
			}		
			if (o.getReferenceKeys().equals(this.getReferenceKeys())){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}

	@Override
	public String toString(){
		return "Reference Keys ?"+ state+ ": " + this.referenceKeys.toString();
	}

	protected void readObject(Input in) throws IOException, ClassNotFoundException{
		int nbRefs = in.readInt();
		for (int i = 0; i <nbRefs ; i++)
		{
			this.addReferenceKey(new ChunkRef(in.readString(), in.readLong(), in.readLong()));    	
		}    	
		this.state=Enum.valueOf(State.class,  in.readString());
	}

	public byte[] serialize() {
		ByteArrayOutputStream baout =  new ByteArrayOutputStream();
		Output kout = new Output(baout);
		writeObject(kout);
		kout.close();
		byte[] res = baout.toByteArray() ;
		return res;
	}

	private void writeObject(Output out) 
	{        
		out.writeInt(0) ; // no registration needed for Kryo
		int version = RedisCacheValue.VERSION ;
		if (version>=1) {
			out.writeInt(-1);
			out.writeInt(version);
		}

		out.writeInt(RedisCacheType.CACHE_REFERENCE_LIST.ordinal());    
		out.writeInt(this.referenceKeys.size());
		for(ChunkRef cr  : this.referenceKeys ){
			out.writeString(cr.referencedKey);
			out.writeLong(cr.lowerBound);
			out.writeLong(cr.upperBound);
		}
		out.writeString(this.state.toString());
	}    

}

