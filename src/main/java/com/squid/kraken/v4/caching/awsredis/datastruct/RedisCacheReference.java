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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RedisCacheReference extends RedisCacheValue {
	
	
	private  String referenceKey ;

	

	public String getReferenceKey() {
		return referenceKey;
	}

	public void setReferenceKey(String referenceKey) {
		this.referenceKey = referenceKey;
	}

	public RedisCacheReference(String ref){
		this.referenceKey = ref;
	}
	
	public RedisCacheReference(){		
	}
	
    @Override
    public boolean equals(Object obj){
    	if (obj instanceof RedisCacheReference ){
    		RedisCacheReference o = (RedisCacheReference) obj;
    		if (o.getReferenceKey().equals(this.getReferenceKey())){
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
    	return "Reference Key : " + this.referenceKey;
    }
    
    protected void readObject(Input in) throws IOException, ClassNotFoundException{
    	this.referenceKey = in.readString();    	
    }
	
    public byte[] serialize() throws IOException{
        ByteArrayOutputStream baout =  new ByteArrayOutputStream();
        Output kout = new Output(baout);
        writeObject(kout);
        kout.close();
        byte[] res = baout.toByteArray() ;
        return res;
    }
    
    private void writeObject(Output out) throws IOException
    {        
    	out.writeInt(0) ; // no registration needed for Kryo
    	int version = RedisCacheValue.VERSION ;
    	if (version>=1) {
    		   
               out.writeInt(-1);
               out.writeInt(version);
           }
    	
        out.writeInt(RedisCacheType.CACHE_REFERENCE.ordinal());    
        out.writeString(this.referenceKey);
    }    
    
}
