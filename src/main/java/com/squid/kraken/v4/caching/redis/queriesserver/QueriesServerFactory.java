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
package com.squid.kraken.v4.caching.redis.queriesserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.caching.redis.AWSRedisCacheConfig;

public class QueriesServerFactory {

	public static final QueriesServerFactory INSTANCE = new QueriesServerFactory();

	static final Logger logger = LoggerFactory.getLogger(QueriesServerFactory.class);

	private static IQueriesServer instServ = null;

	public IQueriesServer getQueriesServer() {
		return instServ;
	}

	public IQueriesServer getNewQueriesServer(AWSRedisCacheConfig conf, boolean onLocalhost) {
		if (instServ == null) {
			if (onLocalhost || conf == null) {
				instServ = new QueriesServer(new AWSRedisCacheConfig());
			} else if (conf.getQueriesServerID().port == -1) {
				instServ = new QueriesServer(conf);
			} else {
				instServ = new QueriesServerStub(conf.getQueriesServerID(), conf.getAppName());
			}
		}
		return instServ;
	}

	/*
	 * public IQueriesServer getQueriesServer(ServerID id, ServerID redisID,
	 * ArrayList<ServerID> workersID, String RedShiftURL, String RSusername,
	 * String RSpassword, boolean onLocalhost ){
	 * 
	 * if(onLocalhost || (id.host.equals("localhost") && id.port==-1)){ return
	 * new QueriesServer(redisID,workersID, RedShiftURL, RSusername,
	 * RSpassword); }else{ return new QueriesServerStub(id); } }
	 */

}
