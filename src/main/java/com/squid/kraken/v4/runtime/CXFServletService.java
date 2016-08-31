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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.velocity.VelocityTemplateManager;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeConfiguration;
import com.squid.kraken.v4.api.core.EmailHelperImpl;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.analytics.BookmarkAnalysisServiceRest;
import com.squid.kraken.v4.api.core.customer.AdminServiceRest;
import com.squid.kraken.v4.api.core.customer.CustomerServiceBaseImpl;
import com.squid.kraken.v4.api.core.customer.CustomerServiceRest;
import com.squid.kraken.v4.api.core.websocket.NotificationWebsocketMetaModelObserver;
import com.squid.kraken.v4.caching.redis.CacheInitPoint;
import com.squid.kraken.v4.caching.redis.RedisCacheConfig;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.config.KrakenConfigV2;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreManagerFactory;
import com.squid.kraken.v4.core.database.impl.DriversService;
import com.squid.kraken.v4.model.Customer;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.DataStoreEventBus;
import com.squid.kraken.v4.persistence.dao.CustomerDAO;
import com.wordnik.swagger.config.ScannerFactory;
import com.wordnik.swagger.jaxrs.config.ReflectiveJaxrsScanner;
import com.wordnik.swagger.models.Info;
import com.wordnik.swagger.models.Swagger;
import com.wordnik.swagger.models.auth.OAuth2Definition;

@SuppressWarnings("serial")
public class CXFServletService extends CXFNonSpringJaxrsServlet {

	private static final Logger logger = LoggerFactory.getLogger(CXFServletService.class);

	@Override
	public void init(ServletConfig config) throws ServletException {
		long ts_start = System.currentTimeMillis();
		logger.info("catalina.base : " + System.getProperty("catalina.base"));

		logger.info(System.getProperty("kraken.cache.config.json"));
		logger.info(System.getProperty("kraken.facet"));
		logger.info(System.getProperty("kraken.config.file"));
		logger.info(System.getProperty("bouquet.config.file"));

		final HashSet<String> facets = new HashSet<String>();

		String facetsStr = System.getProperty("kraken.facet");
		if (facetsStr == null) {
			facetsStr = "front,queries,keysserver,queryworker,cachemanager";
		}
		facets.addAll(Arrays.asList(facetsStr.split(",")));

		// init the CXF Servlet with our specific JaxrsServiceClasses
		ServletConfigDecorator servletConf = new ServletConfigDecorator(config);
		if (facets.contains("front")) {
			// init the API
			logger.info("Facet: Front");
			servletConf.setJaxrsServiceClassesParam(BookmarkAnalysisServiceRest.class.getName() + "," + CustomerServiceRest.class.getName() + ","
					+ AdminServiceRest.class.getName() + "," + "com.wordnik.swagger.jaxrs.listing.ApiListingResource");
		} else {
			servletConf.setJaxrsServiceClassesParam(CacheInitPoint.class.getName());
		}

		initSwagger(config);

		super.init(servletConf);
		//
		extendedInit(facets);
		//

		long ts_end = System.currentTimeMillis();

		logger.info("\n" + "  _                 _                         \n"
				+ " / \\ ._   _  ._    |_)  _       _.      _ _|_ \n"
				+ " \\_/ |_) (/_ | |   |_) (_) |_| (_| |_| (/_ |_ \n"
				+ "     |                           |            ");

		logger.info("Initialization complete in " + (ts_end - ts_start) + "ms");
	}

	private void initVelocity() {
		try {
			// init Velocity Engine using classpath loader (not the file loader)
			VelocityTemplateManager.initEngine();
		} catch (Exception e) {
			logger.error("failed to init velocity engine", e);
			throw e;
		}
	}

	private void extendedInit(HashSet<String> facets) throws ServletException {

		initVelocity();

		// get the version for war Manifest
		String version = null;
		InputStream input = getServletContext().getResourceAsStream("/META-INF/MANIFEST.MF");
		if (input != null) {
			try {
				Manifest manifest = new Manifest(input);
				Attributes mainAttribs = manifest.getMainAttributes();
				version = "{";
				version += " \"build\" : \"" + mainAttribs.getValue("Built-Date") + " ("
						+ mainAttribs.getValue("Revision") + ")";
				version += "\",";
				version += " \"version\" :  \"" + mainAttribs.getValue("Version");
				version += "\"}";
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		RedisCacheConfig conf;

		try {
			String configFile = System.getProperty("kraken.cache.config.json");
			String krakenConfigV2file = System.getProperty("bouquet.config.file");

			if (configFile == null && krakenConfigV2file == null) {
				conf = RedisCacheConfig.getDefault();

			} else {
				if (krakenConfigV2file != null) {
					KrakenConfigV2 krakenConf = KrakenConfigV2.loadFromjson(krakenConfigV2file);
					if (krakenConf.getCache() != null) {
						conf = krakenConf.getCache();
					} else {
						conf = RedisCacheConfig.getDefault();
					}

				} else {
					logger.info(configFile);
					conf = RedisCacheConfig.loadFromjson(System.getProperty("kraken.cache.config.json"));
					logger.info(conf.getAppName());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			conf = RedisCacheConfig.getDefault();
		}

		if (facets.contains("front")) {
			// init the API
			ServiceUtils.getInstance().initAPI(version, 10, 3600 * 24);
			// initialize RedisCacheManager
			RedisCacheManager.getInstance().setConfig(conf);
			RedisCacheManager.getInstance().startCacheManager();
			// init websocket notification
			DataStoreEventBus.getInstance().subscribe(NotificationWebsocketMetaModelObserver.getInstance());
		}
		CacheInitPoint cache = CacheInitPoint.INSTANCE;
		cache.start(conf, facets);
		DriversService.initDriver();

		// DimensionStoreManagerFactory initialization
		try {
			String embeddedValue = KrakenConfig.getProperty("elastic.local", "true");
			boolean embedded = embeddedValue.equals("true");
			ESIndexFacadeConfiguration esConfig = new ESIndexFacadeConfiguration(embedded, null);
			DimensionStoreManagerFactory.init(esConfig);
		} catch (Exception e) {
			logger.error("Failed to initialize DimensionStore with error: " + e.toString());
			throw new ServletException(e);
		}

		// Check if default customer has to be created
		if (System.getProperty("kraken.autocreate") != null) {
			// Extra safety for prod
			if (System.getProperty("kraken.autocreate").contains("true")) {
				AppContext ctx = new AppContext.Builder().build();
				List<Customer> customers = ((CustomerDAO) DAOFactory.getDAOFactory().getDAO(Customer.class))
						.findAll(ctx);
				if (customers.isEmpty()) {
					// create the default Customer
					String defaultClientURL = KrakenConfig.getProperty("default.client.url", true);
					CustomerServiceBaseImpl.getInstance().accessRequestDemo(defaultClientURL,
							EmailHelperImpl.getInstance());
					logger.warn("Default Customer created");
				}
			}
		}
		
		AUTH_MODE authMode = KrakenConfig.getAuthMode();
		if (authMode != AUTH_MODE.OAUTH) {
			logger.warn("AUTH MODE set to "+authMode);
		}

		logger.info("Open Bouquet started with build version : " + ServiceUtils.getInstance().getBuildVersionString());
	}

	public void initSwagger(ServletConfig config) throws ServletException {
		ReflectiveJaxrsScanner scanner = new ReflectiveJaxrsScanner();
		scanner.setResourcePackage("com.squid.kraken.v4.api.core.customer,com.squid.kraken.v4.api.core.analytics");
		ScannerFactory.setScanner(scanner);

		Info info = new Info().title("Bouquet").version("4.2").description("This is Bouquet API");

		ServletContext context = config.getServletContext();
		String basePath = "/" + KrakenConfig.getProperty("kraken.ws.api", "release") + "/"
				+ KrakenConfig.getProperty("kraken.ws.version", "v4.2");
		Swagger swagger = new Swagger().info(info).basePath(basePath);
		logger.info("Swagger base path " + basePath);
		String oauthEndpoint = KrakenConfig.getProperty("kraken.oauth.endpoint",
				"https://api.squidsolutions.com/release/auth/oauth");
		swagger.securityDefinition("kraken_auth",
				new OAuth2Definition().implicit(oauthEndpoint).scope("access", "Access protected resources"));

		context.setAttribute("swagger", swagger);
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		super.destroy();
	}

	static public class ServletConfigDecorator implements ServletConfig {
		private ServletConfig delegate;
		private String jaxrsServiceClassesParam;

		public String getJaxrsServiceClassesParam() {
			return jaxrsServiceClassesParam;
		}

		public void setJaxrsServiceClassesParam(String jaxrsServiceClassesParam) {
			this.jaxrsServiceClassesParam = jaxrsServiceClassesParam;
		}

		public ServletConfigDecorator(ServletConfig delegate) {
			super();
			this.delegate = delegate;
		}

		public String getServletName() {
			return delegate.getServletName();
		}

		public ServletContext getServletContext() {
			return delegate.getServletContext();
		}

		public String getInitParameter(String name) {
			if (name.equals("jaxrs.serviceClasses")) {
				return jaxrsServiceClassesParam;
			} else {
				return delegate.getInitParameter(name);
			}
		}

		@SuppressWarnings("rawtypes")
		public Enumeration getInitParameterNames() {
			return delegate.getInitParameterNames();
		}

	}
}
