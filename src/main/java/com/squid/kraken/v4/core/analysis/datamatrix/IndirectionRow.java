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
package com.squid.kraken.v4.core.analysis.datamatrix;

public class IndirectionRow {

	protected static final Object[] EMPTY_ARRAY = new Object[0];
	
	protected Object[] rawrow;
	
	protected int[] axesIndirection ;
	protected int[] dataIndirection;

	
	public IndirectionRow(){
		
	}
	
	public  IndirectionRow(Object[] rr , int[] aIndir, int[] dIndir){
		this.axesIndirection= aIndir;
		this.dataIndirection= dIndir;
		this.rawrow = rr!=null?rr:EMPTY_ARRAY;

	}
	
	public IndirectionRow(IndirectionRow src){
	//	if (r instanceof IndirectionRow){
//			IndirectionRow src = (IndirectionRow) r;
			this.axesIndirection = src.axesIndirection;
			this.dataIndirection = src.dataIndirection;
			int nbColumns =this.axesIndirection.length + this.dataIndirection.length;
			this.rawrow = new Object[nbColumns];
			System.arraycopy(src.rawrow, 0, this.rawrow, 0, nbColumns) ;

	}
	
	public int size() {
		return this.rawrow!=null?this.rawrow.length:0;
	}
	
	public Object[] getRawRow(){
		return this.rawrow;
	}
	
	public int[] getAxesIndirection(){
		return this.axesIndirection;
	}
	
	public int[] getDataIndirection(){
		return this.dataIndirection;
	}
 
    public Object getAxisValue(int i) {
        return rawrow[axesIndirection[i]];
    }
    
    public int getAxesCount() {
    	if (axesIndirection != null)
    		return axesIndirection.length;
    	else
    		return 0;
    }

    
    public Object getDataValue(int i) {
        return rawrow[dataIndirection[i]];
    }
    
    public Object getValue(int i) {
    	if (i<axesIndirection.length) {
    		return rawrow[axesIndirection[i]];
    	} else {
            return rawrow[dataIndirection[i-axesIndirection.length]];
    	}
    }
    
    public int getDataCount() {
    	if (dataIndirection != null)
    		return dataIndirection.length;
    	else
    		return 0;
    }

	@Override
	public String toString() {
		StringBuilder dump = new StringBuilder("[");
		for (int i=0;i<rawrow.length;i++) {
			if (i>0) dump.append(",");
			dump.append(rawrow[i]==null?"(null)":rawrow[i].toString());
		}
		dump.append("]");
		return dump.toString();
	}
	
}
