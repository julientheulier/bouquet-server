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

import javax.servlet.http.HttpServletRequest;

import org.apache.cxf.common.util.StringUtils;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;

public class JsonDeepReadInInterceptor extends AbstractPhaseInterceptor<Message> {
    public static final String PARAM = "deepread";
    public static final String KEY = "DEEP_READ";
    
    public JsonDeepReadInInterceptor() {
        this(Phase.UNMARSHAL);
    }
    
    public JsonDeepReadInInterceptor(String phase) {
        super(phase);
    }

    public void handleMessage(Message message) throws Fault {
        String value = getHttpRequestParamValue(message);
        if (!StringUtils.isEmpty(value)) {
            message.getExchange().put(KEY, value);
        }
    }

    private String getHttpRequestParamValue(Message message) {
        HttpServletRequest request = (HttpServletRequest) message.get("HTTP.REQUEST");
        return request.getParameter(PARAM);
    }

}
