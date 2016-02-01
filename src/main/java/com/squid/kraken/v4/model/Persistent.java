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

import java.io.Serializable;
import java.util.Set;

import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.visitor.ModelVisitor;
import com.squid.kraken.v4.persistence.AppContext;
import com.wordnik.swagger.annotations.ApiModelProperty;

public interface Persistent<PK extends GenericPK> extends Serializable, HasAccessRights {
    
    /**
     * Get the object Composite Id.
     * @return the object Composite Id.
     */
	@ApiModelProperty(value = "The object Composite Id (Primary Key)")
    public PK getId();
    
    public void setId(PK id);
    
    /**
     * Get the object Id.
     * @return the object Id.
     */
    @ApiModelProperty(value = "The Object Id")
    public String getOid();
    
    public void setOid(String oid);
    
    /**
     * Get the Customer Id
     * @return customerId
     */
    public String getCustomerId();
    
    public void setCustomerId(String customerId);
    
    public String getObjectType();
    
    @ApiModelProperty(value = "The ACL for this object")
    public Set<AccessRight> getAccessRights();
    
    public void setAccessRights(Set<AccessRight> accessRights);
    
    public Role getUserRole();
    
    public void setUserRole(Role role);
    
    /**
     * Model objects may be visited.
     * @param visitor
     */
    public void accept(ModelVisitor visitor);
    
    public Persistent<?> getParentObject(AppContext ctx);
    


}
