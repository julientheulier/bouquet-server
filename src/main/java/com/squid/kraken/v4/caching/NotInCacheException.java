package com.squid.kraken.v4.caching;

import com.squid.kraken.v4.api.core.APIException;

@SuppressWarnings("serial")
public class NotInCacheException extends APIException {

	public NotInCacheException(String message) {
		super(message,  false, ApiError.NOT_IN_CACHE);
	}

    @Override
    protected Integer getErrorCode() {
    	// keeping 204 here for compatibility with the old API
    	return 204 ;
    }
	
}
