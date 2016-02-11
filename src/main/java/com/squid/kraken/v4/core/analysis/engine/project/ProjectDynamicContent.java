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
package com.squid.kraken.v4.core.analysis.engine.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.squid.kraken.v4.core.analysis.engine.cartography.Cartography;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DomainContent;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.Relation;
import com.squid.kraken.v4.model.RelationPK;

/**
 * This class stores the Project dynamic content = Domains & Relations
 * 
 * @author sergefantino
 *
 */
public class ProjectDynamicContent {
	
	private String genkey;
	
	private List<Domain> domains;
	private Map<DomainPK, Domain> domainLookup = new HashMap<DomainPK, Domain>();

	private List<Relation> relations;
	private Map<RelationPK, Relation> relationLookup = new HashMap<RelationPK, Relation>();
	
	private Cartography cartography;
	
	// we use the latch while we are initializing the content
	private CountDownLatch sync = new CountDownLatch(2);
	
	private DomainContentManager domainContentManager = new DomainContentManager();

	public ProjectDynamicContent(String genkey) {
		this.genkey = genkey;
	}
	
	public String getGenkey() {
		return genkey;
	}
	
	public List<Domain> getDomains() {
		return domains;
	}

	/**
	 * look for a domain by its name, or return null if cannot find
	 * @param name
	 * @return the domain or null if cannot find
	 */
	public Domain findDomainByName(String name) {
		for (Domain domain : domains) {
			if (domain.getName().equals(name)) {
				return domain;
			}
		}
		// else
		return null;
	}
	
	public void setDomains(List<Domain> domains) {
		this.domains = domains;
		for (Domain domain : domains) {
			domainLookup.put(domain.getId(), domain);
		}
	}
	
	public Domain get(DomainPK domainPk) {
		return domainLookup.get(domainPk);
	}
	
	public void setRelations(List<Relation> relations) {
		List<Relation> old = this.relations;
		this.relations = relations;
		for (Relation relation : relations) {
			relationLookup.put(relation.getId(), relation);
		}
		if (old==null) sync.countDown();
	}
	
	public List<Relation> getRelations() {
		try {
			sync.await();
			return relations;
		} catch (InterruptedException e) {
			return null;// ?
		}
	}
	
	public Relation get(RelationPK relationPk) {
		try {
			sync.await();
			return relationLookup.get(relationPk);
		} catch (InterruptedException e) {
			return null;// ?
		}
	}
	
	public Cartography getCartography() {
		try {
			sync.await();
			return cartography;
		} catch (InterruptedException e) {
			return null;// ?
		}
	}
	
	public void setCartography(Cartography cartography) {
		Cartography old = this.cartography;
		this.cartography = cartography;
		if (old==null) sync.countDown();
	}
	
	protected DomainContent peekDomainContent(DomainPK id) {
		return this.domainContentManager.peekDomainContent(id);
	}
	
	public DomainContent getDomainContent(Space space) {
		return this.domainContentManager.getDomainContent(space);
	}

	public void release() {
		while (sync.getCount()>0) {
			sync.countDown();
		}
	}
	
}
