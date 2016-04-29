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
package com.squid.kraken.v4;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.kraken.v4.config.KrakenConfigV2;

//import com.squid.kraken.v4.KrakenConfigV2.MailConfig;

/**
 * Kraken configuration helper.<br>
 * Loads properties from an XML file located by a "kraken.config.file" system
 * property.<br>
 * If property if not found, it'll try to get it from System properties.
 */
public class KrakenConfig {

	private static final Logger logger = LoggerFactory
			.getLogger(KrakenConfig.class);

	public static String SYSTEM_PROPERTY_CONFIG_FILE = "kraken.config.file";
	public static String SYSTEM_PROPERTY_CONFIG_FILE_V2 = "bouquet.config.file";

	static Properties props;

	/**
	 * Get a configuration property value
	 * 
	 * @param key
	 *            the property name
	 * @return the property or throws a RuntimeException if the key ins't found.
	 */
	public static synchronized String getProperty(String key) {
		return getProperty(key, false);
	}

	/**
	 * Get a configuration property value
	 * 
	 * @param key
	 *            the property name
	 * @param defaultValue
	 *            a default value
	 * @return the property or the default value if the key ins't found.
	 */
	public static synchronized String getProperty(String key,
			String defaultValue) {
		try {
			return getProperty(key, false);
		} catch (RuntimeException e) {
			return defaultValue;
		}
	}

	/**
	 * Get a configuration property value
	 * 
	 * @param key
	 *            the property name
	 * 
	 * @param allowsNull
	 *            throws a RuntimeException if the key ins't found.
	 * @return the property
	 */
	public static synchronized String getProperty(String key, boolean allowsNull) {
		if (props == null) {
			initProperties();
		}
		// check for a config property
		String p = props.getProperty(key);
		if (p == null) {
			// check for a system property
			p = System.getProperty(key);
		}
		if ((p == null) && (!allowsNull)) {
			throw new RuntimeException(
					"Configuration property '"
							+ key
							+ "' must be declared either as a system property or in the 'kraken.config.file'");
		}
		return p;
	}

	public static synchronized void setProperty(String key, String value) {
		if (props == null) {
			initProperties();
		}
		props.setProperty(key, value);
	}

	public static void initProperties() {
		props = new Properties();
		String filePath = System.getProperty(SYSTEM_PROPERTY_CONFIG_FILE);

		String filePathV2 = System.getProperty(SYSTEM_PROPERTY_CONFIG_FILE_V2);

		logger.info(filePath);

		if (filePath == null && filePathV2 == null) {
			logger.warn("either " + SYSTEM_PROPERTY_CONFIG_FILE + " or "
					+ SYSTEM_PROPERTY_CONFIG_FILE_V2
					+ " system property should be set");
		} else {
			if (filePathV2 != null) {
					try {
						KrakenConfigV2  confV2 = KrakenConfigV2.loadFromjson(filePathV2);
//						logger.info("using conf V2\n" + confV2.toString());
						
						props.setProperty("kraken.ws.host",confV2.getWsHost());
						
						props.setProperty("kraken.mongodb.host",confV2.getMongodb().getHost());
						props.setProperty("kraken.mongodb.port", confV2.getMongodb().getPort());
						props.setProperty("kraken.mongodb.dbname",confV2.getMongodb().getDbname());
						if (confV2.getMongodb().getUser() !=null){
							props.setProperty("kraken.mongodb.user",confV2.getMongodb().getUser());
						}
						if(confV2.getMongodb().getPassword() !=null){
							props.setProperty("kraken.mongodb.password",confV2.getMongodb().getPassword());
						}
						props.setProperty("signup.email.bcc",confV2.getSignupEmailBCC());
						props.setProperty("mail.senderPassword",confV2.getMail().getSenderPassword());
						props.setProperty("mail.hostName",confV2.getMail().getHostname());
						props.setProperty("mail.sslPort", new Integer(confV2.getMail().getSslPort()).toString());		
						props.setProperty("mail.senderName", confV2.getMail().getSenderName());
						props.setProperty("mail.senderEmail",confV2.getMail().getSenderEmail());
						
						props.setProperty("default.client.url",confV2.getDefaultClientURL());
						props.setProperty("kraken.oauth.endpoint",confV2.getKrakenOAuthEndpoint());
						props.setProperty("kraken.ws.api",confV2.getKrakenWSAPI());
						props.setProperty("elastic.local",Boolean.toString(confV2.getElasticLocal()) );
						props.setProperty("feature.dynamic",Boolean.toString(confV2.getFeatureDynamic()));
						
						props.setProperty("ehcache.path",confV2.getEHCachePath());
						
					} catch (IOException e) {
						logger.error("Could not load config file : " + filePathV2);
					}
					

			} else {

				try {
					FileInputStream fis = new FileInputStream(filePath);
					logger.warn("Loading config from : " + filePath);
					load(fis, props);
				} catch (FileNotFoundException e) {
					// try to load from classpath
					logger.warn("Failed, try loading config from classpath");
					try {
						InputStream in = KrakenConfig.class.getClassLoader()
								.getResourceAsStream(filePath);
						load(in, props);
					} catch (Exception e1) {
						logger.error("Could not load config file : " + filePath);
					}
				}
			}
		}
		logger.info("kraken properties " + props.toString());
	}

	private static void load(InputStream in, Properties target) {
		Properties properties = new Properties();
		try {
			properties.loadFromXML(in);
			for (Object key : properties.keySet()) {
				Object value = properties.get(key);
				target.put(key, value);
				if (logger.isDebugEnabled()) {
					logger.debug((key + ":" + value));
				}
			}
		} catch (Exception e) {
			logger.warn("Could not load Kraken config file for stream : " + in,
					e);
		}

	}

}
