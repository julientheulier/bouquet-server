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
package com.squid.kraken.v4.config;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squid.kraken.v4.caching.redis.RedisCacheConfig;
import com.squid.kraken.v4.model.Customer.AUTH_MODE;

public class KrakenConfigV2 {

	static final Logger logger = LoggerFactory.getLogger(KrakenConfigV2.class);

	private RedisCacheConfig cache;

	private String serverMode;
	private String signupEmailBCC;
	private String wsHost;

	// private RestConfig rest;
	private MongodbConfig mongodb;
	private MailConfig mail;
	// public AWSConfig aws;

	public boolean featureDynamic;

	public boolean elasticLocal;

	public String defaultClientURL;
	public String krakenWSAPI;
	public String krakenWSVersion;

	public String krakenOAuthEndpoint;

	public String EHCachePath;
	
	public AUTH_MODE authMode = AUTH_MODE.BYPASS;

	public String getKrakenWSVersion() {
		return krakenWSVersion;
	}

	public void setKrakenWSVersion(String krakenWSVersion) {
		this.krakenWSVersion = krakenWSVersion;
	}

	public KrakenConfigV2() {

	}

	public RedisCacheConfig getCache() {
		return cache;
	}

	public void setCache(RedisCacheConfig redisCacheConfig) {
		this.cache = redisCacheConfig;
	}

	public String getServerMode() {
		return serverMode;
	}

	public void setServerMode(String serverMode) {
		this.serverMode = serverMode;
	}

	public String getSignupEmailBCC() {
		return signupEmailBCC;
	}

	public void setSignupEmailBCC(String signupEmailBCC) {
		this.signupEmailBCC = signupEmailBCC;
	}

	public String getWsHost() {
		return wsHost;
	}

	public void setWsHost(String wSHost) {
		wsHost = wSHost;
	}

	public MongodbConfig getMongodb() {
		return mongodb;
	}

	public void setMongodb(MongodbConfig mongodb) {
		this.mongodb = mongodb;
	}

	public MailConfig getMail() {
		return mail;
	}

	public void setMail(MailConfig mail) {
		this.mail = mail;
	}

	@Override
	public String toString() {
		return "KrakenConfigV2 [cache=" + cache + ", serverMode=" + serverMode + ", signupEmailBCC=" + signupEmailBCC
				+ ", WSHost=" + wsHost + ", mongodb=" + mongodb + ", mail=" + mail + ", featureDynamic="
				+ featureDynamic + ", elasticLocal=" + elasticLocal + ", defaultClientURL=" + defaultClientURL
				+ ", krakenWSAPI=" + krakenWSAPI + ", krakenWSVersion=" + krakenWSVersion + ", krakenOAuthEndpoint="
				+ krakenOAuthEndpoint + "EHCawche path" + EHCachePath + "]";
	}

	public boolean getFeatureDynamic() {
		return featureDynamic;
	}

	public void setFeatureDynamic(boolean featureDynamic) {
		this.featureDynamic = featureDynamic;
	}

	public boolean getElasticLocal() {
		return elasticLocal;
	}

	public void setElasticLocal(boolean elasticLocal) {
		this.elasticLocal = elasticLocal;
	}

	public String getDefaultClientURL() {
		return defaultClientURL;
	}

	public void setDefaultClientURL(String defaultClientURL) {
		this.defaultClientURL = defaultClientURL;
	}

	public String getKrakenWSAPI() {
		return krakenWSAPI;
	}

	public void setKrakenWSAPI(String krakenWSAPI) {
		this.krakenWSAPI = krakenWSAPI;
	}

	public String getKrakenOAuthEndpoint() {
		return krakenOAuthEndpoint;
	}

	public void setKrakenOAuthEndpoint(String krakenOAuthEndpoint) {
		this.krakenOAuthEndpoint = krakenOAuthEndpoint;
	}

	public String getEHCachePath() {
		return EHCachePath;
	}

	public void setEHCachePath(String eHCachePath) {
		EHCachePath = eHCachePath;
	}

	public AUTH_MODE getAuthMode() {
		return authMode;
	}

	public void setAuthMode(AUTH_MODE authMode) {
		this.authMode = authMode;
	}

	public static KrakenConfigV2 loadFromjson(String filename) throws IOException {

		File file = new File(filename);
		if (!file.exists() || !file.canRead()) {
			logger.info("can't read config file " + filename);
			logger.info("can't read config file " + file.getAbsolutePath());
			logger.info("exists? " + file.exists());
			logger.info("canRead? " + file.canRead());
			return null;
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		return mapper.readValue(file, KrakenConfigV2.class);
	}

}
