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

import org.mongodb.morphia.annotations.Index;
import org.mongodb.morphia.annotations.Indexes;
import org.mongodb.morphia.annotations.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.squid.kraken.v4.api.core.JobResult;
import com.squid.kraken.v4.persistence.AppContext;
import io.swagger.annotations.ApiModelProperty;

@SuppressWarnings("serial")
@Indexes( @Index("temporary, creationTime") )
public abstract class JobBaseImpl<PK extends GenericPK, R extends JobResult> extends PersistentBaseImpl<PK> implements ComputationJob<PK, R> {

    private Status status;

    @ApiModelProperty(readOnly = true)
    private Error error;

    @ApiModelProperty(readOnly = true)
    private Statistics statistics;

    @ApiModelProperty(readOnly = true)
    private Long resultsSize;
    
    private Boolean temporary;
    
    private Boolean autoRun;
    
    @Transient
	@ApiModelProperty(readOnly = true)
    private transient R results;
    
    @ApiModelProperty(readOnly = true)
    private Long creationTime;

    public JobBaseImpl(PK id) {
        super(id);
        // default values
    	this.temporary = true;
    	this.autoRun = true;
    }

    /**
     * The current state of this job.<br>
     * 
     * @return the following Status values:
     *         <ul>
     *         <li>PENDING - Queued</li>
     *         <li>RUNNING - Running</li>
     *         <li>DONE - Completed, either successfully or not. If unsuccessful, the error field should not be null</li>
     *         </ul>
     */
    @JsonProperty
    public Status getStatus() {
        return status;
    }

    @JsonProperty
    public Error getError() {
        return error;
    }

    @JsonProperty
    public Statistics getStatistics() {
        return statistics;
    }

    @JsonIgnore
    public void setStatus(Status status) {
        this.status = status;
    }

    @JsonIgnore
    public void setError(Error error) {
        this.error = error;
    }

    @JsonIgnore
    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    @JsonProperty
    public Long getResultsSize() {
        return resultsSize;
    }

    @JsonIgnore
    public void setResultsSize(Long resultsSize) {
        this.resultsSize = resultsSize;
    }

    public Boolean getTemporary() {
        return temporary;
    }

    public void setTemporary(Boolean temporary) {
        this.temporary = temporary;
    }

	public Boolean getAutoRun() {
		return autoRun;
	}

	public void setAutoRun(Boolean autoRun) {
		this.autoRun = autoRun;
	}
	
	public Long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(Long creationTime) {
		this.creationTime = creationTime;
	}

	@JsonProperty
	public R getResults() {
		return results;
	}

	@JsonIgnore
	public void setResults(R results) {
		this.results = results;
	}

	@Override
    public abstract Persistent<?> getParentObject(AppContext ctx);
}
