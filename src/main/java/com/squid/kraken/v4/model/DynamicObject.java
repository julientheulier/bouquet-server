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
package com.squid.kraken.v4.model;

import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * A dynamic object is an object that has been automatically generated by the DynamicManager.
 * 
 * @author sergefantino
 *
 * @param <PK>
 */
public abstract class DynamicObject<PK extends GenericPK> extends LzPersistentBaseImpl<PK> implements Cloneable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -3633197187753335496L;

	/**
	 * default is false, you need to explicitly set the dynamic flag when creating the object
	 * (previous versions had a transient flag)
	 */
    private boolean isDynamic2 = false;
    
    public DynamicObject() {
		super(null);
	}

	public DynamicObject(PK id) {
		super(id);
	}

	public DynamicObject(PK id, boolean isDynamic) {
		super(id);
		this.isDynamic2 = isDynamic;
	}
    
	public DynamicObject(PK id, String name) {
		super(id, name);
	}

    public DynamicObject(PK id, String name,
			boolean isDynamic) {
    	super(id, name);
    	this.isDynamic2 = isDynamic;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	/**
     * check if the dimension is Dynamic
     * @return
     */
    @ApiModelProperty(hidden=true, value = "the dynamic flag indicates if the object is automatically generated")
    public boolean isDynamic() {
		return isDynamic2;
	}
    
    public void setDynamic(boolean isDynamic) {
		this.isDynamic2 = isDynamic;
	}


}