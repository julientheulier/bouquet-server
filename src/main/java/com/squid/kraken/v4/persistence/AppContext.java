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
package com.squid.kraken.v4.persistence;

import java.util.List;

import com.squid.kraken.v4.api.core.APIException;
import com.squid.kraken.v4.model.AccessToken;
import com.squid.kraken.v4.model.CustomerPK;
import com.squid.kraken.v4.model.User;

public class AppContext {

    private final CustomerPK customerPk;

    private final User user;

    private final boolean dryRun;

    private final boolean noError;

    private final String locale;

    private final boolean refresh;

    private final AccessToken token;
    
    private final String clientId;
    
    private final boolean deepRead;
    
    private final List<String> options;

    public AppContext(Builder b) {
        this.customerPk = b.customerPk;
        this.user = b.user;
        this.dryRun = b.dryRun;
        this.noError = b.noError;
        this.locale = b.locale;
        this.refresh = b.refresh;
        this.token = b.token;
        this.deepRead = b.deepRead;
        this.clientId = b.clientId;
        this.options = b.options;
    }

    public String getCustomerId() {
    	if (customerPk != null) {
    		return customerPk.getCustomerId();
    	} else {
    		return null;
    	}
    }

    public CustomerPK getCustomerPk() {
        return customerPk;
    }

    public User getUser() {
        return user;
    }

    /**
     * Optional : used to dry-run database queries.
     */
    public boolean isDryRun() {
        return dryRun;
    }

    /**
     * Optional : used to invalidate caches.
     */
    public boolean isRefresh() {
        return refresh;
    }

    /**
     * Optional : used to set 'noError' mode to {@link APIException}
     */
    public boolean isNoError() {
        return noError;
    }
    
    /**
     * Optional : return full object graph when reading.
     */
    public boolean isDeepRead() {
        return deepRead;
    }

    /**
     * Locale as specified by the request.
     * 
     * @return the specified locale or null.
     */
    public String getLocale() {
        return locale;
    }

    public AccessToken getToken() {
        return token;
    }
    
    public String getClientId() {
    	return clientId;
    }

    /**
     * Optional : a list of option parameters.
     */
    public List<String> getOptions() {
		return options;
	}

	@Override
    public String toString() {
        return "AppContext [customerId=" + customerPk.getCustomerId() + ", dryRun=" + dryRun + ", locale=" + locale
                + ", noError=" + noError + ", user=" + user + "]";
    }

    static public class Builder {

        private final CustomerPK customerPk;
        
        private final String clientId;

        private final User user;

        private boolean dryRun = false;

        private boolean noError = false;

        private String locale;

        private boolean refresh = false;
        
        private final AccessToken token;
        
        private boolean deepRead = false;
        
        private List<String> options;
        
        /**
         * Anomynous (no user) context.
         */
        public Builder() {
            this.customerPk = null;
            this.user = null;
            this.token = null;
            this.clientId = null;
        }
        
        /**
         * Anomynous (no user) context.
         * @param customerId
         */
        public Builder(String customerId, String clientId) {
            if (customerId != null) {
                this.customerPk = new CustomerPK(customerId);
            } else {
                this.customerPk = null;
            }
            this.user = null;
            this.token = null;
            this.clientId = clientId;
        }

        public Builder(AccessToken token, User user) {
            this.customerPk = new CustomerPK(token.getCustomerId());
            this.clientId = token.getClientId();
            this.user = user;
            this.token = token;
        }

        public Builder(String customerId, User user) {
            this(new CustomerPK(customerId), user);
        }

        public Builder(CustomerPK customerPK, User user) {
            super();
            this.customerPk = customerPK;
            this.user = user;
            this.token = null;
            this.clientId = null;
        }

        public Builder(AppContext ctx) {
            super();
            this.customerPk = ctx.getCustomerPk();
            this.user = ctx.getUser();
            this.dryRun = ctx.isDryRun();
            this.noError = ctx.isNoError();
            this.locale = ctx.getLocale();
            this.refresh = ctx.isRefresh();
            this.token = ctx.getToken();
            this.deepRead = ctx.isDeepRead();
            this.clientId = ctx.getClientId();
        }

        public AppContext build() {
            AppContext ctx = new AppContext(this);
            return ctx;
        }

        public Builder setDryRun(boolean dryRun) {
            this.dryRun = dryRun;
            return this;
        }

        public Builder setNoError(boolean noError) {
            this.noError = noError;
            return this;
        }

        public Builder setLocale(String locale) {
            this.locale = locale;
            return this;
        }

        public Builder setRefresh(boolean refresh) {
            this.refresh = refresh;
            return this;
        }

        public Builder setDeepRead(boolean deepRead) {
            this.deepRead = deepRead;
            return this;
        }
        
        public Builder setOptions(List<String> options) {
			this.options = options;
			return this;
		}
        
    }

}
