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
package com.squid.kraken.v4.api.core;

import com.squid.kraken.v4.model.AccessRight.Role;

/**
 * This class contains all the access rights, it also defines the priority between these rights. The priority is
 * described in ASC order as below:
 * <ul>
 * <li>1: READ</li>
 * <li>2: WRITE</li>
 * <li>3: OWNER</li>
 * </ul>
 * 
 * @author danhddv
 * 
 */
public class RolePriorityMapper implements Comparable<RolePriorityMapper> {

    /**
     * Role.
     * 
     * @see com.squid.kraken.v4.model.AccessRight.Role
     */
    private Role role;

    /**
     * Priority of the role.
     */
    private int priority;

    public RolePriorityMapper(Role role) {
        this.role = role;
        switch (role) {
        case OWNER:
            priority = 3;
            break;
        case WRITE:
            priority = 2;
            break;
        case READ:
            priority = 1;
            break;
        }
    }

    @Override
    public int compareTo(RolePriorityMapper rolePriorityMapper) {
        if (this.priority == rolePriorityMapper.getPriority()) {
            return 0;
        } else if (this.priority > rolePriorityMapper.getPriority()) {
            return -1;
        } else {
            return 1;
        }
    }

    /**
     * Get the priority of a given role.
     * 
     * @return priority of a given role
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Get the role.
     * 
     * @return role
     */
    public Role getRole() {
        return role;
    }

}
