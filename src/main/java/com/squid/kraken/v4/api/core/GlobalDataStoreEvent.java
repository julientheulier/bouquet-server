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

import java.io.Serializable;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.persistence.DataStoreEvent;
import com.squid.kraken.v4.persistence.DataStoreEvent.Type;


/**
 * A {@link DataStoreEvent} wrapper to be persisted in the shared event queue.
 */
@Entity(value = GlobalEventPublisher.EVENTBUS, noClassnameStored = true)
public class GlobalDataStoreEvent implements Serializable {

    private static final long serialVersionUID = 4424781302038215507L;
    
    @Id
    private ObjectId id;

    @Embedded
    private GenericPK sourceId;
    
    private Type type;
    
    private long ts;
    
    private String server;
    
    public GlobalDataStoreEvent() {
        
    }

    public GlobalDataStoreEvent(GenericPK eventId, Type type, long ts, String server) {
        super();
        this.sourceId = eventId;
        this.ts = ts;
        this.server = server;
        this.type = type;
    }

    public GenericPK getSourceId() {
        return sourceId;
    }

    public void setSourceId(GenericPK sourceId) {
        this.sourceId = sourceId;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }
    
}
