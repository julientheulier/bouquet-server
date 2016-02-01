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
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.cfg.AnnotationBundleKey;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.jaxrs.json.JsonEndpointConfig;
import com.squid.kraken.v4.osgi.DeepReadView;
import com.squid.kraken.v4.osgi.JsonDeepReadPreStreamInterceptor;

/**
 * Squid specific implementation made to support per-request JsonViews such as {@link DeepReadView}.
 * Forked from Jackson - 2.4.0.
 */
public class CustomJacksonJsonProvider extends JacksonJsonProvider {
	
	private static final Annotation DEEP_READ_ANNOTATION = new DeepReadAnnotation();

	/**
     * Method that JAX-RS container calls to serialize given value.
     */
    @Override
    public void writeTo(Object value, Class<?> type, Type genericType, Annotation[] annotations,
            MediaType mediaType,
            MultivaluedMap<String,Object> httpHeaders, OutputStream entityStream) 
        throws IOException
    {
    	
    	// BEIGIN -- SQUID : check if we have to specify a Deep-read view
    	Class<?> view;
    	Annotation[] annotations2;
    	
        if (httpHeaders.containsKey(JsonDeepReadPreStreamInterceptor.KEY_DEEP_READ)) {
        	view = DeepReadView.class;
        	// add the deep read annotation
            List<Annotation> annotationsList = new ArrayList<Annotation>();
            annotationsList.addAll(Arrays.asList(annotations));
            annotationsList.add(DEEP_READ_ANNOTATION);
            annotations2 = new Annotation[annotationsList.size()];
            annotations2 = annotationsList.toArray(annotations2);
            // cleanup
            httpHeaders.remove(JsonDeepReadPreStreamInterceptor.KEY_DEEP_READ);
        } else {
        	view = Object.class; // important
        	annotations2 = annotations;
        }
        // END -- SQUID
    
        AnnotationBundleKey key = new AnnotationBundleKey(annotations2, type);
        JsonEndpointConfig endpoint;
        synchronized (_writers) {
            endpoint = _writers.get(key);
        }
        // not yet resolved (or not cached any more)? Resolve!
        if (endpoint == null) {
        	ObjectMapper mapper = locateMapper(type, mediaType);
        	mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        	mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            endpoint = _configForWriting(mapper, annotations, view);
            // and cache for future reuse
            synchronized (_writers) {
                _writers.put(key.immutableKey(), endpoint);
            }
        }

        // Any headers we should write?
        _modifyHeaders(value, type, genericType, annotations, httpHeaders, endpoint);
        
        ObjectWriter writer = endpoint.getWriter();

        // Where can we find desired encoding? Within HTTP headers?
        JsonEncoding enc = findEncoding(mediaType, httpHeaders);
        JsonGenerator g = _createGenerator(writer, entityStream, enc);
        
        try {
            // Want indentation?
            if (writer.isEnabled(SerializationFeature.INDENT_OUTPUT)) {
                g.useDefaultPrettyPrinter();
            }
            JavaType rootType = null;

            if (genericType != null && value != null) {
                /* 10-Jan-2011, tatu: as per [JACKSON-456], it's not safe to just force root
                 *    type since it prevents polymorphic type serialization. Since we really
                 *    just need this for generics, let's only use generic type if it's truly
                 *    generic.
                 */
                if (genericType.getClass() != Class.class) { // generic types are other impls of 'java.lang.reflect.Type'
                    /* This is still not exactly right; should root type be further
                     * specialized with 'value.getClass()'? Let's see how well this works before
                     * trying to come up with more complete solution.
                     */
                    rootType = writer.getTypeFactory().constructType(genericType);
                    /* 26-Feb-2011, tatu: To help with [JACKSON-518], we better recognize cases where
                     *    type degenerates back into "Object.class" (as is the case with plain TypeVariable,
                     *    for example), and not use that.
                     */
                    if (rootType.getRawClass() == Object.class) {
                        rootType = null;
                    }
                }
            }

            // Most of the configuration now handled through EndpointConfig, ObjectWriter
            // but we may need to force root type:
            if (rootType != null) {
                writer = writer.withType(rootType);
            }
            value = endpoint.modifyBeforeWrite(value);

            // [Issue#32]: allow modification by filter-injectible thing
            ObjectWriterModifier mod = ObjectWriterInjector.getAndClear();
            if (mod != null) {
                writer = mod.modify(endpoint, httpHeaders, value, writer, g);
            }

            writer.writeValue(g, value);
        } finally {
            g.close();
        }
    }
	
	
}