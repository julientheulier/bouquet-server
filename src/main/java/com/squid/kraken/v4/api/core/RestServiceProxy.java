package com.squid.kraken.v4.api.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.jaxrs.client.JAXRSClientFactory;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

public class RestServiceProxy<T> {

    public T newInstance(Class<T> c, String apiURI) {
        List<Object> providers = new ArrayList<Object>();
        providers.add(new JacksonJsonProvider());
        providers.add(new ExceptionsMapper());
        return JAXRSClientFactory.create(apiURI, c,
                providers, true);
    }

}