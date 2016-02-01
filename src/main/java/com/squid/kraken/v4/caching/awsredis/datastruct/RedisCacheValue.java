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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.squid.kraken.v4.caching.awsredis.RedisCacheException;

public class RedisCacheValue {

	
	public enum RedisCacheType{
		RAW_MATRIX, CACHE_REFERENCE
	}
	
	public static final int VERSION = 2;
	
	public RedisCacheValue(){
	}
	
    public static RedisCacheValue deserialize(byte[] serializedVal) throws IOException, ClassNotFoundException{
        Input in = new Input( new ByteArrayInputStream(serializedVal));
        
        HashMap <String, Integer> registration = new HashMap <String, Integer>() ;

        //READ HEADER
        
        //classes registration (used by Kryo)
        int nbClasses = in.readInt();
        for (int i = 0; i <nbClasses; i++){
            String className = in.readString();
            int id  = in.readInt();
            registration.put(className, id);
        }
        
        // get version         
        int position = in.position(); // in case there is no version available;
        
        int version;
        int type ;
        int check_version = in.readInt();
               
        if (check_version==-1) {
            // version>=1, read the version in the stream
            version = in.readInt();// read the version
            if (version<2){
            	type = RedisCacheType.RAW_MATRIX.ordinal();
            }else{
            	type = in.readInt() ;
            }
        } else {
            // version=0, no information in stream
        	version = 0;
        	type = RedisCacheType.RAW_MATRIX.ordinal();
        	in.setPosition(position) ;
        }
                
        //delegate deserialization to the concrete  class
        if (type == RedisCacheType.RAW_MATRIX.ordinal()){
            RawMatrix res =  new RawMatrix(version, registration );
            res.readObject(in) ;
            in.close();
            return res;

        }else{
        	if (type == RedisCacheType.CACHE_REFERENCE.ordinal()){
        		RedisCacheReference  res = new RedisCacheReference();
        		res.readObject(in);
        		in.close();
        		return res;
        	
        	}else{
        		throw new ClassNotFoundException("Could not deserialize Redis Cache Value");
        	}
    	}        
    }

}
