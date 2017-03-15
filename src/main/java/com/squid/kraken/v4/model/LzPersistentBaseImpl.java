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

import org.mongodb.morphia.annotations.Property;

import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.ApiModelProperty;

/**
 * A Localized {@link PersistentBaseImpl}.
 * @see HasLocalizedName
 */
@SuppressWarnings("serial")
public abstract class LzPersistentBaseImpl<PK extends GenericPK> extends PersistentBaseImpl<PK> implements
        HasLocalizedName, Cloneable {

    @Property("name")
    private String name;
    
    @Property("description")
    private String description;

    public LzPersistentBaseImpl(PK id) {
        super(id);
    }

    public LzPersistentBaseImpl(PK id, String name) {
        super(id);
        this.name = name;
    }
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    /**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

    /**
     * The localized version of the name (localized according to {@link AppContext}'s locale).
     */
    
    @ApiModelProperty
    public String getLName() {
        return name;
    }

    public void setLName(String LName) {
        this.name = LName;
    }

}
