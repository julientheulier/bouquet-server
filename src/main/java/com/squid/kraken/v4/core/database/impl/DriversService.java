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

package com.squid.kraken.v4.core.database.impl;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.impl.DriverLoader;
import com.squid.core.database.impl.DriverShim;

public class DriversService {

	static final Logger logger = LoggerFactory.getLogger(DriversService.class);
	
	// Drivers NON JDBC 4 Compliant (aka does not contains a MANIFEST for
	// java.sql.Driver)
	public static final String[] drivers = new String[] //{};
			{ "org.apache.drill.jdbc.Driver",
			"org.apache.hive.jdbc.HiveDriver" };

	// Without servlet directory or configs
	public synchronized static void initDriver() {
		
		// copy class loader to rollback
		ClassLoader rollback = Thread.currentThread().getContextClassLoader();

		DriverLoader dd = DriverLoader.getDriverLoader();
		Thread.currentThread().setContextClassLoader(dd);
		// Will detect automatically JDBC 4 Compliant;
		// Drill and Spark JDBC plugins are not.
		ServiceLoader<java.sql.Driver> loader = ServiceLoader.load(java.sql.Driver.class, dd);
		Iterator<Driver> driverIt = loader.iterator();
		// LoggerFactory.getLogger(this.getClass()).debug("List of vendorSupport
		// Providers");
		while (driverIt.hasNext()) {
			try {
				Driver driver = driverIt.next();
				Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
				Boolean duplicate = false;
				while (availableDrivers.hasMoreElements()) {
					Driver already = availableDrivers.nextElement();
					if (already instanceof DriverShim) {
						if (((DriverShim) already).getName() == driver.getClass().getName()) {
							duplicate = true;
						}
					}
				}
				if (logger.isDebugEnabled()) {
					logger.debug("driver available " + driver.getClass());
				}
				;
				if (!duplicate) {
					DriverShim shim = new DriverShim(driver);
					DriverManager.registerDriver(shim);
				}
				;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			for (String driverName : drivers) {
				Driver driver = (Driver) Class.forName(driverName, true, dd).newInstance();
				Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
				Boolean duplicate = false;
				while (availableDrivers.hasMoreElements()) {
					Driver already = availableDrivers.nextElement();
					if (already instanceof DriverShim) {
						if (((DriverShim) already).getName() == driver.getClass().getName()) {
							duplicate = true;
						}
					}
				}
				if (logger.isDebugEnabled()) {
					logger.debug("driver available " + driver.getClass());
				}
				;
				if (!duplicate) {
					DriverShim shim = new DriverShim(driver);
					DriverManager.registerDriver(shim);
				}
				;
			}

		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Thread.currentThread().setContextClassLoader(rollback);
	}

}
