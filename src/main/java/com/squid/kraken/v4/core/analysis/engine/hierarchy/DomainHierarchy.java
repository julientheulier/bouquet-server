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
package com.squid.kraken.v4.core.analysis.engine.hierarchy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.domain.IDomain;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.api.core.AccessRightsUtils;
import com.squid.kraken.v4.api.core.ObjectNotFoundAPIException;
import com.squid.kraken.v4.caching.redis.RedisCacheManager;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.engine.project.ProjectManager;
import com.squid.kraken.v4.core.analysis.scope.AnalysisScope;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.model.AccessRight.Role;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionPK;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.DomainPK;
import com.squid.kraken.v4.model.DynamicObject;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.model.LzPersistentBaseImpl;
import com.squid.kraken.v4.model.Metric;
import com.squid.kraken.v4.model.MetricExt;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * This is the Hierarchy for a given Domain
 * <li>it manages the DimensionIndexes
 * @author sergefantino
 *
 */
public class DomainHierarchy {
    
    static final Logger logger = LoggerFactory.getLogger(DomainHierarchy.class);

    private DomainHierarchyCompute compute;
    private Space root;
    private List<List<DimensionIndex>> structure = null;
    
    /**
     * list all the domain indexes including proxy
     */
    private List<DimensionIndex> flatten = null;
    private HashMap<Axis, DimensionIndex> lookup = new HashMap<>();

    private List<DimensionIndex> segments = new ArrayList<>();
    
    private Set<String> dependencies = null;
    
    private String objectName = "";
    private String genKey;
    private State state;
  
    public enum State{	
    	NEW, INIT, STARTED, CANCELLED, DONE
    }

    protected DomainHierarchy(){
        this.state = State.NEW;
    }
    
    protected DomainHierarchy(Space root, List<List<DimensionIndex>> structure) {
    	this();
        this.root = root;
        this.structure = structure;
        loadHierarchies(root, structure);
    }
    
    /**
     * allow to add more hierarchy - this is used for lazy sub-domains intialization
     * @param hierarchy
     */
    protected void addHierarchy(List<DimensionIndex> hierarchy) {
    	this.structure.add(hierarchy);
        loadHierarchies(root, structure);
    }
    
    public String getVersion() {
		return genKey;
	}
    
    private DomainContent getContent() {
    	try {
			return ProjectManager.INSTANCE.getDomainContent(root);
		} catch (ScopeException e) {
			throw new RuntimeException(e);
		}
    }

    /**
     * return any Dimension, Metric or Relation that can be referenced by the name - or null if nothing found
     * @param name
     */
	public Object findByName(String name) {
		for (Dimension check : getContent().getDimensions()) {
			if (check.getName()!=null && check.getName().equals(name)) {
				return check;
			}
		}
		for (Metric check : getContent().getMetrics()) {
			if (check.getName()!=null && check.getName().equals(name)) {
				return check;
			}
		}
		try {
			for (Space next : root.S()) {
				if (next.getRelationName()!=null && next.getRelationName().equals(name)) {
					return next.getRelation();
				}
			}
		} catch (ComputingException | ScopeException e) {
			// ignore
		}
		// looks ok
		return null;
	}

	public List<Dimension> getDimensions(AppContext ctx) {
    	List<Dimension> filtered = new ArrayList<Dimension>();
    	for (Dimension dim : getContent().getDimensions()) {
    		if (hasRole(ctx, dim)) {
    			filtered.add(cloneWithRole(ctx, dim));
    		}
    	}
    	return filtered;
    }
    
    /**
     * get the dimension identified by its PK - or throw an Exception if cannot find or cannot access
     * @param ctx
     * @param dimensionPK
     * @return
     * @throws ScopeException
     */
    public Dimension getDimension(AppContext ctx, DimensionPK dimensionPK) throws ScopeException {
    	Dimension result = findDimension(ctx, dimensionPK);
    	if (result!=null) {
    		return result;
    	} else {
    		throw new ScopeException("cannot find dimension with PK="+dimensionPK);
    	}
    }

    /**
     * find the dimension identified by its PK - or null if cannot find or cannot access
     * @param ctx
     * @param dimensionPK
     * @return
     * @throws ScopeException
     */
    public Dimension findDimension(AppContext ctx, DimensionPK dimensionPK) throws ScopeException {
    	for (Dimension dim : getContent().getDimensions()) {
    		if (dim.getId().equals(dimensionPK)) {
    			checkRole(ctx, dim);
    			return cloneWithRole(ctx, dim);
    		}
    	}
    	// else
    	return null;
    }
    
    public Metric findMetric(AppContext ctx, String metricID) {
    	for (Metric metric : getContent().getMetrics()) {
    		if (metric.getId().getMetricId().equals(metricID)) {
    			checkRole(ctx, metric);
    			return cloneWithRole(ctx, metric);
    		}
    	}
    	// else
    	return null;
    }

    
    public Metric getMetric(AppContext ctx, String metricID) {
    	Metric metric = findMetric(ctx, metricID);
    	if (metric!=null) {
    		return metric;
    	} else {
        	throw new ObjectNotFoundAPIException("cannot find metric with ID="+metricID, true);
    	}
    }
    
    public List<Metric> getMetrics() {
    	return Collections.unmodifiableList(getContent().getMetrics());
    }
    
    public List<Metric> getMetrics(AppContext ctx) {
    	List<Metric> filtered = new ArrayList<Metric>();
    	for (Metric metric : getContent().getMetrics()) {
    		if (hasRole(ctx, metric)) {
    			filtered.add(cloneWithRole(ctx, metric));
    		}
    	}
    	return filtered;
    }
    
    public List<Metric> getMetrics(AppContext ctx, boolean showDynamics) {
    	List<Metric> filtered = new ArrayList<Metric>();
    	for (Metric metric : getContent().getMetrics()) {
    		if (!showDynamics && metric.isDynamic()) {
    			// ignore
    		} else if (hasRole(ctx, metric)) {
    			filtered.add(cloneWithRole(ctx, metric));
    		}
    	}
    	return filtered;
    }
    
    public List<MetricExt> getMetricsExt(AppContext ctx) {
    	List<MetricExt> filtered = new ArrayList<MetricExt>();
    	for (Metric metric : getContent().getMetrics()) {
    		if (hasRole(ctx, metric)) {
    			String definition = root.prettyPrint()+"."+"["+AnalysisScope.MEASURE.getToken()+":'"+metric.getName()+"']";
    			boolean isVisible = root.getDomain().isDynamic() || !metric.isDynamic();
    			MetricExt copy = new MetricExt(metric, definition, isVisible);
    			AccessRightsUtils.getInstance().setRole(ctx, copy);
    			filtered.add(copy);
    		}
    	}
    	return filtered;
    }
    
    private boolean hasRole(AppContext ctx, DynamicObject<?> dynamic) {
    	// T15 rules
    	Role role;
    	if (dynamic.isDynamic()) {
    		role = Role.WRITE;
    	} else {
    		role = Role.READ;
    	}
    	return AccessRightsUtils.getInstance().hasRole(ctx, dynamic, role);
    }
    
    private void checkRole(AppContext ctx, DynamicObject<?> dynamic) {
    	// T15 rules
    	Role role;
    	if (dynamic.isDynamic()) {
    		role = Role.WRITE;
    	} else {
    		role = Role.READ;
    	}
    	AccessRightsUtils.getInstance().checkRole(ctx, dynamic, role);
    }

	private <TYPE extends LzPersistentBaseImpl<? extends GenericPK>> TYPE cloneWithRole(AppContext ctx, TYPE obj) {
		try {
			TYPE copy = (TYPE)obj.clone();
			AccessRightsUtils.getInstance().setRole(ctx, copy);
			return copy;
		} catch (CloneNotSupportedException e) {
			return obj;
		}
	}

	public void setCompute(DomainHierarchyCompute c){
    	this.compute = c;
    }
    
    public boolean isDone(Integer timeoutMs) throws InterruptedException, TimeoutException, ExecutionException {
    	
    	if ((this.state == State.CANCELLED) ||(this.state == State.DONE)){
    		return true;
    	}else{
    		if( this.state== State.STARTED){
    			return compute.isDone(timeoutMs);
    		}else{
    			return false;
    		}
    	}
    }

    public boolean isDone(DimensionIndex index, Integer timeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
    	
    	if ((this.state == State.CANCELLED) ||(this.state == State.DONE)){
    		return true;
    	}else{
    		if( this.state== State.STARTED){
    	   		return compute.isDone(index, timeoutMs);
    	   	}else{
    	   		return false;
    		}
    	}
    }
   
    public boolean isValid() {
        String newKey = genKey();
/*    	logger.info("is " +root.getDomain().getName() +" valid ? old key:" + this.genKey + ", newKey:" + newKey);
    	logger.info("dependencies " +  this.dependencies.toString()); */
        if (newKey==null) {
            return true;
        } else if ( newKey.equals(this.genKey)) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * cancel the domain computation if needed - wait at least 10s to make sure the computation is cancelled
     */
   protected void cancel() {
	   if (compute != null) {
		   if (this.state== State.STARTED) {// working on
			   compute.cancel();
			   try {
				   isDone(10000);// wait 10s
			   } catch (TimeoutException | InterruptedException | ExecutionException e) {
				   logger.warn("failed to cancel computing the hierarchy for domain '"+root+"' in less than 10s...");
			   }
		   }
	   }
    }
    
    private void loadHierarchies(Space root, List<List<DimensionIndex>> structure) {
    	//
    	// clear global object since now we may load the hierarchy several times
        this.dependencies = new HashSet<>();
        this.flatten = new ArrayList<>();
        this.segments = new ArrayList<>();
        //
        this.objectName = "H/" + root.getDomain().getId().toUUID();
        // flatten
        HashSet<DomainPK> domains = new HashSet<DomainPK>();// list the sub-domains
        // add the project to dependencies to support refreshDB
        dependencies.add(root.getUniverse().getProject().getId().toUUID());
        // add the main domain
        domains.add(root.getDomain().getId());
        dependencies.add(root.getDomain().getId().toUUID());
        // add every linked dimension
        for (List<DimensionIndex> hierarchy : structure) {
            for (DimensionIndex index : hierarchy) {
                this.add(index);
                this.dependencies.add(index.getDimension().getId().toUUID());
                // add sub-domain if needed
                if (index instanceof DimensionIndexProxy) {
                	DimensionIndexProxy proxyIndex =(DimensionIndexProxy) index;
                	DimensionIndex proxy = proxyIndex.getSourceIndex();
                	boolean ok = false; 
                	while (!ok ) {
                        Domain parent = proxy.getAxis().getParent().getRoot();
                        if (!domains.contains(parent.getId())) {
                            domains.add(parent.getId());
                            // add hierarchy dep
                            dependencies.add("H/" + parent.getId().toUUID());
                        }
                        if (!(proxy instanceof DimensionIndexProxy)){
                        	ok = true;
                        }	else{
                    	 proxyIndex =(DimensionIndexProxy) proxy;
                    	 proxy = proxyIndex.getSourceIndex();
                        }
                	}
                }
            }
        }
        // add the segments
		for (DimensionIndex i : flatten) {
			if (i.getAxis().getDimension().getImageDomain()
					.isInstanceOf(IDomain.CONDITIONAL)
					&& (i.getParent() == null) && i.getChildren().isEmpty()) {
				segments.add(i);
			}
		}
        // compute the version
		updateGenKey();
        this.setState(State.INIT);
    }
    
    public State getState() {
		return state;
	}
    
	public void setState(State state) {
		synchronized(this.state){
			this.state = state;
		}
	}

    private void updateGenKey() {
        genKey = genKey();
    }
    
	private String genKey() {
        return RedisCacheManager.getInstance().getKey(objectName,dependencies).getStringKey();
    }
    
    public Space getRoot() {
        return root;
    }
    
    public List<List<DimensionIndex>> getStructure() {
        return structure;
    }
    
    /**
     * Populate the hierarchy with the indexes
     * 
     * <li>this method is not thread-safe
     * @param index
     * @return
     */
    private void add(DimensionIndex index) {
        flatten.add(index);
        lookup.put(index.getAxis(), index);
    }

    /**
     * get the index associated with an axis
     * @param axis
     * @return the index or NULL if not defined (this may happens if the model changed during a transaction?)
     */
    public DimensionIndex getDimensionIndex(Axis axis) {
        DimensionIndex check = lookup.get(axis);
        if (check==null) {
        	//logger.error("cannot lookup DimensionIndex for Axis "+axis);
        }
        return check;
    }

    /**
     * get the domain indexes, including proxy 
     * @return
     */
    public List<DimensionIndex> getDimensionIndexes() {
        return Collections.unmodifiableList(flatten);
    }

    /**
     * get the visible domain indexes for that context, including proxy 
     * @return
     */
    public List<DimensionIndex> getDimensionIndexes(AppContext ctx) {
    	ArrayList<DimensionIndex> indexes = new ArrayList<DimensionIndex>(flatten.size());
        for (DimensionIndex index : flatten) {
        	if (index.isVisible() && hasRole(ctx, index.getDimension())) {
        		indexes.add(index);
        	}
        }
        return indexes;
    }
    
    // segments
    public List<DimensionIndex> getSegments(AppContext ctx) {
    	if (segments.isEmpty()) {
    		return Collections.emptyList();
    	} else {
    		List<DimensionIndex> filter = new ArrayList<DimensionIndex>();
    		for (DimensionIndex index : segments) {

    			if (index.isVisible()) {// show dynamics only if the domain is dynamic
	    			if (hasRole(ctx, index.getDimension())) {
	    				filter.add(index);
	    			}
    			}
    		}
    		return filter;
    	}
    }
    
    public boolean isSegment(DimensionIndex index) {
        if (segments==null) {
            return false;
        } else {
            return segments.contains(index);
        }
    }
    
    @Override
    public String toString() {
    	return "DomainHierarchy '"+getRoot().getDomain().getName()+"'";
    }

}
