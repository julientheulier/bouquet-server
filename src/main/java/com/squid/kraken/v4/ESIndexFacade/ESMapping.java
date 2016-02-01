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
package com.squid.kraken.v4.ESIndexFacade;

public class ESMapping {

	
	public enum ESIndexMapping{
		ANALYZED, NOT_ANALYZED, NO, BOTH
	}
	
	public enum ESTypeMapping{
		STRING, LONG, DOUBLE, BOOLEAN, DATE,
	}
	
	public String typename ;
	public ESIndexMapping index;
	public ESTypeMapping type;
	
	public ESMapping(){};
	
	public ESMapping(String typename, ESIndexMapping index, ESTypeMapping type){
		this.typename = typename;
		this.index = index ;
		this.type = type;				
	};
	
	
	public String getIndex(){
		switch(this.index){
		case ANALYZED:
			return "analyzed";
		case NO:
			return "no";
		case NOT_ANALYZED:
			return "not_analyzed";
		default:
			return "analyzed";
		}		
	}
	
	public String getType(){
		switch(this.type){
			case BOOLEAN:
				return "boolean";
			case LONG:
				return "long";
			case DOUBLE:
				return "double";
			case DATE:
				return "date";
			case STRING:
				return "string";
			default:
				return "string";
		}
	}

    @Override
    public String toString() {
        return "ESMapping [typename=" + typename + ", index=" + index
                + ", type=" + type + "]";
    }

}
