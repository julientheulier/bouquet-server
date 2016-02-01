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
package com.squid.kraken.v4.runtime;

import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.endpoint.Server;
import org.apache.cxf.interceptor.Interceptor;
import org.apache.cxf.interceptor.LoggingInInterceptor;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.message.Message;
import org.apache.cxf.rs.security.cors.CrossOriginResourceSharingFilter;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transport.common.gzip.GZIPOutInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.squid.core.velocity.VelocityTemplateManager;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.ExceptionsMapper;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.customer.CustomerServiceRest;
import com.squid.kraken.v4.osgi.JsonDeepReadInInterceptor;
import com.squid.kraken.v4.osgi.JsonDeepReadPreStreamInterceptor;

public class StandaloneService {
    
    private static final Logger logger = LoggerFactory.getLogger(StandaloneService.class);
    private static Server server;

    static public void main(String[] args) throws Exception {
    	
    	// init Velocity Engine
        VelocityTemplateManager.initEngine(System.getProperty("user.dir")+"/WebContent/");
        
        JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();
        // Set the providers
        List<Object> providers = new ArrayList<Object>();
        // using Jackson instead of default Jettison
        providers.add(new JacksonJsonProvider());
        // customer exceptions mapper
        providers.add(new ExceptionsMapper());
        // Cross-Origin support
        providers.add(new CrossOriginResourceSharingFilter());
        sf.setProviders(providers);
        
        // register the services
        List<Object> services = new ArrayList<Object>();
        services.add(CustomerServiceRest.getInstance());
        sf.setServiceBeans(services);
        
        List<Interceptor<? extends Message>> inInterceptors = sf.getInInterceptors();
        List<Interceptor<? extends Message>> outInterceptors = sf.getOutInterceptors();
        
        // Deep-read View support
        inInterceptors.add(new JsonDeepReadInInterceptor());
        outInterceptors.add(new JsonDeepReadPreStreamInterceptor());
        
        // GZIP support
        inInterceptors.add(new GZIPInInterceptor());
        outInterceptors.add(new GZIPOutInterceptor());

        // enable requests logging (only if in debug mode)
        if (logger.isDebugEnabled()) {
            inInterceptors.add(new LoggingInInterceptor());
        }

        // run the server
        String krakenHost = KrakenConfig.getProperty("kraken.ws.host");
        if (krakenHost == null) {
            krakenHost = "kraken:9000";
        }
        sf.setAddress("http://"+krakenHost+"/dev/v4.2/");
        server = sf.create();
        logger.info("started CXF server : " + server.getEndpoint());
        
        // init the API
        ServiceUtils.getInstance().initAPI("version",10, 3600*24);
        
        String version = null;
        logger.info("Kraken server started - version : "+version);
    }

}
