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
package com.squid.kraken.v4.api.core.websocket;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import javax.websocket.DecodeException;
import javax.websocket.Decoder;
import javax.websocket.EncodeException;
import javax.websocket.Encoder;
import javax.websocket.EndpointConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WebsocketJSONCoder<T> implements Encoder.TextStream<T>,
		Decoder.TextStream<T> {

	private Class<T> _type;

	// ObjectMapper is not thread safe
	private ThreadLocal<ObjectMapper> _mapper = new ThreadLocal<ObjectMapper>() {

		@Override
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};

	@Override
	public void init(EndpointConfig endpointConfig) {

	}

	@Override
	public void encode(T object, Writer writer) throws EncodeException,
			IOException {
		_mapper.get().writeValue(writer, object);
	}

	@Override
	public T decode(Reader reader) throws DecodeException, IOException {
		return _mapper.get().readValue(reader, _type);
	}

	@Override
	public void destroy() {

	}

}