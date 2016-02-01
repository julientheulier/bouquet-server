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
package com.squid.kraken.v4.persistence;

import java.util.ArrayList;
import java.util.List;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.Annotation;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.model.ProjectFacetJob;
import com.squid.kraken.v4.model.ProjectUser;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.Shortcut;
import com.squid.kraken.v4.model.State;
import com.squid.kraken.v4.model.User;
import com.squid.kraken.v4.model.UserGroup;

/**
 * MongoDB connection management.<br>
 * Handles the following config properties :
 * <ul>
 * <li><tt>kraken.mongodb.host</tt></li>
 * <li><tt>kraken.mongodb.port</tt></li>
 * <li><tt>kraken.mongodb.dbname</tt></li>
 * <li><tt>kraken.mongodb.user</li>
 * <li><tt>kraken.mongodb.password</li>
 * </ul>
 */
public class MongoDBHelper {

	private static final Logger logger = LoggerFactory
			.getLogger(MongoDBHelper.class);

	public static final String OPERATOR_SET = "$set";
	public static final String OPERATOR_INC = "$inc";

	private static Datastore dataStore;

	private static MongoClient getMongo(String mongoName) {
		MongoClient mongoInstance;

		String mongoHostProp = KrakenConfig.getProperty("kraken.mongodb.host");
		String[] mongoHostList = mongoHostProp.split(",");

		String mongoPortProp = KrakenConfig.getProperty("kraken.mongodb.port");
		String[] mongoPortList = mongoPortProp.split(",");

		MongoClientOptions opt = MongoClientOptions.builder()
				.connectionsPerHost(250).build();
		List<ServerAddress> addrs = new ArrayList<ServerAddress>();
		for (int i = 0; i < mongoHostList.length; i++) {
			int mongoPort = Integer.parseInt(mongoPortList[i]);
			addrs.add(new ServerAddress(mongoHostList[i], mongoPort));
		}

		// Auth

		String mongoUser = KrakenConfig.getProperty("kraken.mongodb.user",
				true);
		String mongoPwd = KrakenConfig.getProperty(
				"kraken.mongodb.password", true);

		logger.info("kraken.mongodb.dbname : " + mongoName);
		logger.info("kraken.mongodb.user : " + mongoUser);
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		if (mongoUser != null) {
			credentials.add(MongoCredential.createMongoCRCredential(
					mongoUser, mongoName, mongoPwd.toCharArray()));
		}

		mongoInstance = new MongoClient(addrs, credentials, opt);

		return mongoInstance;
	}
	
	/**
	 * Create a single MongoDB Morphia Datastore.
	 * 
	 * @return a Datastore.
	 */
	private synchronized static Datastore createDatastore() {
		if (dataStore == null) {

			Morphia m = new Morphia();
			String mongoName = KrakenConfig
					.getProperty("kraken.mongodb.dbname");
			dataStore = m.createDatastore(getMongo(mongoName), mongoName);

			// map entities (required for indexes creation)
			m.map(Customer.class);
			m.map(Project.class);
			m.map(Attribute.class);
			m.map(Dimension.class);
			m.map(Domain.class);
			m.map(ProjectFacetJob.class);
			m.map(Metric.class);
			m.map(Client.class);
			m.map(User.class);
			m.map(UserGroup.class);
			m.map(ProjectAnalysisJob.class);
			m.map(AccessToken.class);
			m.map(Relation.class);
			m.map(Annotation.class);
			m.map(ProjectUser.class);
			m.map(State.class);
			m.map(Shortcut.class);

			// creates indexes from @Index annotations in your entities
			try {
				dataStore.ensureIndexes();
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		return dataStore;
	}

	/**
	 * Get a MongoDB Morphia entry point.
	 * 
	 * @return a Morphia Datastore.
	 */
	public static Datastore getDatastore() {
		if (dataStore == null) {
			return createDatastore();
		}
		return dataStore;
	}

}