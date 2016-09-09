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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Striped;
import com.squid.core.database.impl.DataSourceReliable;
import com.squid.core.database.impl.DatabaseServiceException;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;

public class HikariDataSourceReliable extends HikariDataSource implements DataSourceReliable {
    static final Logger logger = LoggerFactory.getLogger(HikariDataSourceReliable.class);

    private final RateLimiter rateLimiter = RateLimiter.create(20.0);
	private final Striped<Semaphore> semaphore;

	public HikariDataSourceReliable(int maxPoolSize) {
        super();
        this.semaphore = Striped.semaphore(1, maxPoolSize); //same than the number of connection in the pool
        super.setMaximumPoolSize(maxPoolSize);
        super.setMinimumIdle(0);// limit the number of idle connection to the minimum, this is not a bottleneck for Bouquet use-case
    }

    public HikariDataSourceReliable(String driversPath) {
    	this(3);
    }
    
    @Override
    public void setMaximumPoolSize(int maxPoolSize) {
    	throw new RuntimeException("changing the poolSize is not supported");
    }

	public void releaseSemaphore(){
		semaphore.get("Resource Limit").release(1);
	}

    @Override
    public Connection getConnection() throws SQLException {
        if (!rateLimiter.tryAcquire(1, TimeUnit.SECONDS)) {
            throw new DatabaseServiceException("Unable to Acquire the lock for connection");
        }
		try {
			semaphore.get("Resource Limit").acquire(1);
			return super.getConnection();
		} catch (InterruptedException e) {
			throw new SQLException("Interrupted while getting connection resources");
		}
    }


	public Connection getConnectionBlocking() throws SQLException {
        if(!rateLimiter.tryAcquire(1, TimeUnit.SECONDS)){
            throw new DatabaseServiceException("Unable to Acquire the lock for connection");
        }
		if (super.getJdbcUrl().contains("drill") || super.getJdbcUrl().contains("hive") ) {
			try {
				semaphore.get("Resource Limit").acquire(1);
				Connection conn = super.getConnection();
				logger.warn("JDBC plugin does not support isValid method; No blocking Connection;");
				// Not every DB support isValid.
				if(conn!=null){
					return conn;
				}else{
					throw new DatabaseServiceException("unable to create a connection");
				}
			} catch (InterruptedException e) {
				throw new DatabaseServiceException("Interrupted while getting connection resources");
			} catch (SQLException e) {
				throw new DatabaseServiceException(
						"An error occured while connecting to the project database: "
								+ e.getMessage(), e);
			}
		} else {
			Connection conn = null;
			int attempts = 0;
			while (attempts<3 && (conn==null || !conn.isValid(3000))) {
				try {
					semaphore.get("Resource Limit").acquire(1);
					conn = super.getConnection();
				} catch (InterruptedException e) {
					throw new DatabaseServiceException("Interrupted while getting connection resources");
				} catch (PoolInitializationException e) {
					// T665 - unwrap exception coming from HikariCP
					Throwable cause = e;
					if (cause.getCause()!=null) {
						cause = cause.getCause();
						if (cause.getCause()!=null) {
							cause = cause.getCause();
						}
					}
					if (cause!=null && cause instanceof SQLTransientException) {
						throw new DatabaseServiceException("unable to connect to " + super.getJdbcUrl() + "\n" + cause.getMessage(), cause);
					} else {
						throw new DatabaseServiceException("unable to connect to " + super.getJdbcUrl() + "\n" + cause.getMessage(), cause);
					}
				} catch (Throwable e) {
					throw new DatabaseServiceException("unable to connect to " + super.getJdbcUrl() + "\n" + e.getMessage(), e);
				}
				attempts++;
			}
			// Not every DB support isValid.
			if (conn!=null) {
				return conn;
			} else {
				throw new DatabaseServiceException("unable to connect to " + super.getJdbcUrl());
			}
		}
	}
}
