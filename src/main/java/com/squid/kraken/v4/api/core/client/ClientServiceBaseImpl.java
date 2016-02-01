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
package com.squid.kraken.v4.api.core.client;

import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.jose4j.jwk.RsaJsonWebKey;
import org.jose4j.jwk.RsaJwkGenerator;
import org.jose4j.lang.JoseException;

import com.squid.kraken.v4.api.core.GenericServiceImpl;
import com.squid.kraken.v4.model.Client;
import com.squid.kraken.v4.model.ClientPK;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.dao.ClientDAO;

public class ClientServiceBaseImpl extends GenericServiceImpl<Client, ClientPK> {

	private static ClientServiceBaseImpl instance;

	public static ClientServiceBaseImpl getInstance() {
		if (instance == null) {
			instance = new ClientServiceBaseImpl();
		}
		return instance;
	}

	private ClientServiceBaseImpl() {
		// made private for singleton access
		super(Client.class);
	}

	public List<Client> readAll(AppContext ctx) {
		ClientDAO dao = (ClientDAO) factory.getDAO(Client.class);
		return dao.findAll(ctx);
	}
	
	@Override
	public Client store(AppContext ctx, Client newInstance) {
		// init the secret
		newInstance.setSecret(UUID.randomUUID().toString());
		// Generate an RSA key pair, which will be used for signing and
		// verification of the JWT, wrapped in a JWK
		try {
			RsaJsonWebKey key = RsaJwkGenerator.generateJwk(2048);
			String privateKey = encodeToPEM(key.getRsaPrivateKey().getEncoded(), "PRIVATE");
			newInstance.setJWTKeyPrivate(privateKey);
			String publicKey = encodeToPEM(key.getRsaPublicKey().getEncoded(), "PUBLIC");
			newInstance.setJWTKeyPublic(publicKey);
			return super.store(ctx, newInstance);
		} catch (JoseException e) {
			throw new RuntimeException(e);
		}
	}
	
	private String encodeToPEM(byte[] keyBytes, String name) {
		String key = Base64.encodeBase64String(keyBytes);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < key.length(); i++) {
			if ((i % 65) == 0) {
				sb.append("\n");
			}
			sb.append(key.charAt(i));
		}
		String keyFormatted = sb.toString();
		if (!keyFormatted.endsWith("\n")) {
			keyFormatted = keyFormatted + "\n";
		}
		keyFormatted = "-----BEGIN RSA " + name + " KEY-----" + keyFormatted
				+ "-----END RSA " + name + " KEY-----";
		return keyFormatted;
	}
	
}
