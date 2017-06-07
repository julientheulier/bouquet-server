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
package com.squid.kraken.v4.api.core;

@SuppressWarnings("serial")
public class ObjectNotFoundAPIException extends APIException {

    public ObjectNotFoundAPIException(boolean noError) {
        super(noError);
    }

    public ObjectNotFoundAPIException(String message, boolean noError) {
        super(message, noError);
    }

    public ObjectNotFoundAPIException(String message, Throwable cause, boolean noError) {
        super(message, cause, noError);
    }

    public ObjectNotFoundAPIException(Throwable cause) {
        this(cause, false);
    }

    public ObjectNotFoundAPIException(Throwable cause, boolean noError) {
        super(cause, noError);
    }

    @Override
    protected Integer getErrorCode() {
    	return 404;
    }
    
}
