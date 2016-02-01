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

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Event bus for Datastore operations such as object creation, update, delete.
 */
public class DataStoreEventBus {

    static private DataStoreEventBus instance;

    static synchronized public DataStoreEventBus getInstance() {
        if (instance == null) {
            instance = new DataStoreEventBus();
        }
        return instance;
    }

    private Collection<DataStoreEventObserver> DAOobservers;
    private Collection<DataStoreEventObserver> observers;

    public DataStoreEventBus() {
    	DAOobservers = new ConcurrentLinkedQueue<DataStoreEventObserver>();
        observers = new ConcurrentLinkedQueue<DataStoreEventObserver>();
    }

    public void publishEvent(DataStoreEvent event) {
        for (DataStoreEventObserver observer : DAOobservers) {
            observer.notifyEvent(event);
        }
        for (DataStoreEventObserver observer : observers) {
            observer.notifyEvent(event);
        }
    }

    // T269
    public void subscribe(PersistentDAO<?, ?> observer) {
    	DAOobservers.add(observer);
    }

    public <T extends DataStoreEventObserver> T subscribe(T observer) {
        observers.add(observer);
        return observer;
    }
    
    public <T extends DataStoreEventObserver> T unSubscribe(T observer) {
        observers.remove(observer);
        return observer;
    }

}
