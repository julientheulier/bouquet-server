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
package com.squid.kraken.v4.model;

import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.LocalizationDataStoreDecorator;
import io.swagger.annotations.ApiModelProperty;

/**
 * Marks the object has having a name which can be Localized.<br>
 * This localized name is handled by having one attribute 'name' which is the name value using the default locale and a
 * LName attribute which contains the localized name value.<br>
 * Only the 'name' attribute will be persisted along with the object, the other localized values will be stored in a
 * specific {@link LString} object and injected at runtime by a {@link LocalizationDataStoreDecorator}.<br>
 * On the Rest API side, only the Lname value will be made visible.
 */
public interface HasLocalizedName {

    /**
     * The {@link AppContext#DEFAULT_LOCALE} name
     */
    @XmlTransient
    @JsonIgnore
    public String getName();

    @XmlTransient
    @JsonIgnore
    public void setName(String name);

    /**
     * The localized version of the name (localized according to {@link AppContext}'s locale).
     */
    @ApiModelProperty(position = 1)
    @JsonProperty("name")
    public String getLName();

    @JsonProperty("name")
    public void setLName(String lname);

}
