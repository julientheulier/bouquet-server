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
package com.squid.kraken.v4.osgi;

import java.util.Collections;

import org.apache.cxf.common.util.StringUtils;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.jaxrs.impl.MetadataMap;
import org.apache.cxf.message.Exchange;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;

public class JsonDeepReadPreStreamInterceptor extends AbstractPhaseInterceptor<Message> {
	
	public static final String KEY_DEEP_READ = "Deep-read";
    
    public JsonDeepReadPreStreamInterceptor() {
        super(Phase.PRE_STREAM);
    }
    
    public void handleMessage(Message message) throws Fault {
        String value = getValue(message);
        if (!StringUtils.isEmpty(value)) {
            setHeaders(message);
        }
    }

    @SuppressWarnings("unchecked")
    protected void setHeaders(Message message) {
        MetadataMap headers = (MetadataMap) message.get(Message.PROTOCOL_HEADERS);
        if (headers == null) {
            headers = new MetadataMap();
            message.put(Message.PROTOCOL_HEADERS, headers);
        }
        headers.put(KEY_DEEP_READ, Collections.singletonList("true"));  
    }

    private String getValue(Message message) {
        Exchange exchange = message.getExchange();
        return (String) exchange.get(JsonDeepReadInInterceptor.KEY);
    }

}
