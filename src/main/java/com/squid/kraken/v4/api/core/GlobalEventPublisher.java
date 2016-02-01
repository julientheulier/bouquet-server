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

import java.util.Iterator;

import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEventBus;
import com.squid.kraken.v4.persistence.DataStoreEventObserver;
import com.squid.kraken.v4.persistence.MongoDBHelper;

/**
 * Distributes events across servers.<br>
 * Observes the {@link DataStoreEventBus} and publishes {@link DataStoreEvent}s
 * against a message queue shared by all servers.<br>
 * Polls the shared message queue for incoming events from other servers and
 * publish it to the local {@link DataStoreEventBus}. Note : Currently only
 * source object ids (pks) are published<br>
 * It uses a MongoDB capped collection named "eventbus" to store the queue and a tailable cursor
 * to subscribe to notifications.<br>
 */
public class GlobalEventPublisher implements DataStoreEventObserver {

	final Logger logger = LoggerFactory.getLogger(GlobalEventPublisher.class);

	static public final String TS_FIELD = "ts";
	static public final String EVENTBUS = "eventbus";
	static private GlobalEventPublisher instance;

	private final String serverUUID;

	private Reader reader;

	private Thread readerThread;
	
	private final DataStoreEventBus eventBus;

	/**
	 * Create a new event publisher having "kraken.ws.host" property as server UUID.
	 */
	static public synchronized GlobalEventPublisher getInstance() {
		if (instance == null) {
			// set the server id
			String serverUUID = KrakenConfig.getProperty("kraken.ws.host");
			// create the singleton
			instance = new GlobalEventPublisher(serverUUID, DataStoreEventBus.getInstance());
		}
		return instance;
	}

	public GlobalEventPublisher(String serverUUID, DataStoreEventBus eventBus) {
		this.eventBus = eventBus;
		this.serverUUID = serverUUID;

		// get the DB
		DB mongo = MongoDBHelper.getDatastore().getDB();

		// Create the capped collection
		final BasicDBObject conf = new BasicDBObject("capped", true);
		conf.put("size", 100000);
		if (!mongo.collectionExists(EVENTBUS)) {
			logger.info("creating capped collection : " + EVENTBUS);
			mongo.createCollection(EVENTBUS, conf);
		}

		logger.info("GlobalEventPublisher started with serverUUID : " + serverUUID);
	}

	/**
	 * Start polling for new messages in the queue.
	 */
	public void start(long docId) {
		// create the reader thread
		reader = new Reader(serverUUID, eventBus, docId);
		readerThread = new Thread(reader);
		readerThread.start();
	}

	/**
	 * Stop polling for new messages in the queue.
	 */
	public void stop() {
		if (reader!=null) reader.stop();
	}

	public String getServerUUID() {
		return serverUUID;
	}

	/**
	 * Publish to the global queue incoming events.<br>
	 * Only internal events will be published.<br>
	 * 
	 * @param event
	 *            incoming event.
	 */
	@Override
	public void notifyEvent(DataStoreEvent event) {
		if (!event.isExternal()) {
			GenericPK pk = null;
			if (event.getSource() instanceof GenericPK) {
				pk = (GenericPK) event.getSource();
			}
			if (event.getSource() instanceof Persistent<?>) {
				pk = ((Persistent<?>) event.getSource()).getId();
			}
			if (pk != null) {
				// insert a new event
				GlobalDataStoreEvent devent = new GlobalDataStoreEvent(pk,
						event.getType(), System.currentTimeMillis(),serverUUID);
				MongoDBHelper.getDatastore().save(devent);
			}
		}
	}

	/**
	 * The thread that is reading from the capped collection.
	 */
	private static class Reader implements Runnable {

		private static final int SLEEP_TIME = 100;

		final Logger logger = LoggerFactory.getLogger(Reader.class);

		private boolean running;
		
		private final DataStoreEventBus eventBus;
		
		private final String serverUUID;
		
		private long docId;

		public Reader(String serverUUID, DataStoreEventBus eventBus, long docId) {
			this.serverUUID = serverUUID;
			this.eventBus = eventBus;
			this.docId = docId;
			running = false;
		}

		public void stop() {
			running = false;
		}

		@Override
		public void run() {
			if (!running) {
				running = true;
			} else {
				return;
			}
			logger.info("Starting reader thread");
			
			long sleepTime;

			while (running) {
				sleepTime = SLEEP_TIME;
				try {
					Iterator<GlobalDataStoreEvent> cur = createCursor(docId);
					while (cur.hasNext() && running) {
						GlobalDataStoreEvent doc = cur.next();
						docId = doc.getTs();
						if (logger.isDebugEnabled()) {
							if(logger.isDebugEnabled()){logger.debug(("read doc : " + doc));}
						}
						if (doc.getSourceId() != null) {
							// Publish the event to the DataStoreEventBus
							eventBus.publishEvent(
									new DataStoreEvent(null, doc.getSourceId(),
											doc.getType(), true));
						}
					}
				} catch (Throwable t) {
					// increase sleep time to avoid flooding
					sleepTime = SLEEP_TIME*10;
					logger.warn(t.getMessage(), t);
				}

				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException ie) {
					running = false;
				}
				
			}
			logger.info("Reader thread stopped");
		}

		private Iterator<GlobalDataStoreEvent> createCursor(Long ts) {
			Query<GlobalDataStoreEvent> query = MongoDBHelper.getDatastore()
					.createQuery(GlobalDataStoreEvent.class);
			query.field(TS_FIELD).greaterThan(ts);
			query.field("server").notEqual(serverUUID);
			Iterator<GlobalDataStoreEvent> it = query.tail();
			if (!it.hasNext()) {
				// init the cursor by inserting an empty event
				GlobalDataStoreEvent devent = new GlobalDataStoreEvent(null,
						null, ts+10, null);
				MongoDBHelper.getDatastore().save(devent);
			}
			return it;
		}
	}

}
