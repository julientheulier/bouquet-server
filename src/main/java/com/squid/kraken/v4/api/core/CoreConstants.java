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

/**
 * This class contains constants.
 */
public class CoreConstants {

    public static String CONSOLE_CLIENT_NAME = "Admin Console";
    public static String CONSOLE_CLIENT_ID = "admin_console";
    
    public static String DASHBOARD_CLIENT_NAME = "Dashboard";
    public static String DASHBOARD_CLIENT_ID = "dashboard";

    /**
     * Param name customer_id.
     */
    public static String PARAM_NAME_CUSTOMER_ID = "customerId";

    /**
     * Name of the "guest" group which is created by default when a new project is created.
     */
    public static final String PRJ_DEFAULT_GROUP_GUEST = "guest_";

    /**
     * Name of the "admin" group which is created by default when a new project is created.
     */
    public static final String PRJ_DEFAULT_GROUP_ADMIN = "admin_";

	/** 
	 * super users group
	 */
	public static final String CUSTOMER_GROUP_SUPER = "superuser";

	/** 
	 * administrators group
	 */
	public static final String CUSTOMER_GROUP_ADMIN = "admin";

}
