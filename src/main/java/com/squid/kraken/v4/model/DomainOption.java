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

import java.io.Serializable;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonInclude;

public class DomainOption implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7243859790416372462L;


	public final static String LINK_PREFIX = "LINKED_";

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public static String getLinkPrefix() {
		return LINK_PREFIX;
	}

	public String getDestSchema() {
		return destSchema;
	}

	public void setDestSchema(String destSchema) {
		this.destSchema = destSchema;
	}

	public String getDestProjectId() {
		return destProjectId;
	}

	public void setDestProjectId(String destProjectId) {
		this.destProjectId = destProjectId;
	}

	public Boolean getAlink() {
		return alink;
	}

	public void setAlink(Boolean alink) {
		this.alink = alink;
	}

	public String getSourceProjectId() {
		return sourceProjectId;
	}

	public void setSourceProjectId(String sourceProjectId) {
		this.sourceProjectId = sourceProjectId;
	}

	public String getShortcut() {
		return shortcut;
	}

	public void setShortcut(String shortcut) {
		this.shortcut = shortcut;
	}

	/*
             * Used to create the domain (useful for transfered domain).
             * TODO Implement composition to express multiple iterations.
             */
	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private String shortcut = null;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private ProjectAnalysisJob analysisJob = null;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private String sourceProjectId = null;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private String destSchema = null;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private String destProjectId = null;


	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private Boolean reinjected = false;

	// Mostly for domains that are linked to this one.
	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	private ArrayList<DomainPK> dependencies = new ArrayList<DomainPK>();

	/*
	 * True if the domain was created as a link
	 */
	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public Boolean alink = false;

	@JsonInclude(JsonInclude.Include.NON_DEFAULT)
	public String LinkSource = null;

	public DomainOption() {
		// TODO Auto-generated constructor stub
	}

	public Boolean getReinjected() {
		return reinjected;
	}

	public void setReinjected(Boolean reinjected) {
		this.reinjected = reinjected;
	}

	public String getLinkSource() {
		return LinkSource;
	}

	public void setLinkSource(String linkSource) {
		LinkSource = linkSource;
	}

	public ArrayList<DomainPK> getDependencies() {
		return dependencies;
	}

	public void setDependencies(ArrayList<DomainPK> dependencies) {
		this.dependencies = dependencies;
	}

	public void addDependency(DomainPK dependency) {
		dependencies.add(dependency);
	}

	public ProjectAnalysisJob getAnalysisJob() {
		return analysisJob;
	}

	public void setAnalysisJob(ProjectAnalysisJob analysisJob) {
		this.analysisJob = analysisJob;
	}
}
