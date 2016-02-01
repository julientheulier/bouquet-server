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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.database.model.Table;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.ESIndexFacade.ESIndexFacadeException;
import com.squid.kraken.v4.api.core.ServiceUtils;
import com.squid.kraken.v4.api.core.attribute.AttributeServiceBaseImpl;
import com.squid.kraken.v4.caching.awsredis.RedisCacheProxy;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStore;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreException;
import com.squid.kraken.v4.core.analysis.engine.index.IDimensionStore;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreManagerFactory;
import com.squid.kraken.v4.core.analysis.engine.index.IndexationException;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Universe;
import com.squid.kraken.v4.core.expression.visitor.ExtractTables;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.DimensionOption;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.persistence.AppContext;

public class DimensionIndex {
	
	static final Logger logger = LoggerFactory
			.getLogger(DimensionIndex.class);

	public enum Status {
        STALE,// need to refresh
        DONE,
        ERROR
     	};
    
	private DimensionIndex parent = null;
	private List<DimensionIndex> children = null;
	
	private Axis axis;
	private Dimension dimension;
	
	private List<DimensionIndex>  proxies ;
	
	// in case of a dynamic dimension, this is the raw path (krkn-110)
	private String dimensionPath = null;
	
	// override the model dimension name
	private String dimensionName = null;
	
	private Status status = Status.STALE;
	private String error_message = null;
	
	private IDimensionStore store = null;

    private List<Attribute> attribute_cache = null;// cache the attribute to avoid model changes
    private int attr_count;
	private int displayIndex = -1;// allow to use an attribute as the display value
	
	private DimensionOption fullOptions = null;// merge all options (krkn-61)
	
	public IDimensionStore getStore() {
			return this.store;
	}
	
	protected void setStore(IDimensionStore store) {
	    if (this.store==null) {
	        this.store = store;
	    } else {
	        throw new IllegalStateException("cannot change the store definition");
	    }
	}
	
	// cannot define it as a constructor
	protected void _DimensionIndex(DimensionIndex parent, Axis axis) {
		this.axis = axis;
        this.dimension = axis.getDimension();
        init(dimension);
        //
        this.status = readStatus();
        //
        setParent(parent);
	}
	
	public DimensionIndex(){
		this.proxies = new ArrayList<DimensionIndex>();
	}
	
	public DimensionIndex(DimensionIndex parent, Axis axis) throws InterruptedException, DimensionStoreException {
		this();
	    _DimensionIndex(parent, axis);
	    
	    if ( this.dimension.getType().equals(Dimension.Type.INDEX)){
	    	logger.info(dimension.getName() +  "  of type INDEX : do not index in ES" );
	    	this.status = Status.DONE ;
	    	this.store = (IDimensionStore) new DimensionStore(this);
	    }else{    
        //
	    	this.store = DimensionStoreManagerFactory.INSTANCE.createIndexStore(this);
	    }
	}
	

	/**
	 * Internal constructor, allow to bypass the store allocation
	 * @param parent
	 * @param axis
	 * @param store
	 */
	protected DimensionIndex(DimensionIndex parent, Axis axis, IDimensionStore store) {
		this();
	    _DimensionIndex(parent, axis);
        //
        this.store = store;
        if (store!=null && this.store.isCached()) {
            this.status = Status.DONE;
        }
	}
	
	public boolean isProxy() {
		return false;
	}
	
	private void init(Dimension dimension) {
		// init the attributes
        // using the root ctx so we can see all attributes whatever the access rights is
        AppContext ctx = ServiceUtils.getInstance().getRootUserContext(dimension.getCustomerId());
        this.attribute_cache = AttributeServiceBaseImpl.getInstance().readAll(ctx, dimension.getId());
        this.attr_count = this.attribute_cache.size();
        // set the displayIndex if _value attribute
        int i=0;
        for (Attribute attr : attribute_cache) {
        	if (attr.getName().equalsIgnoreCase("_value")) {
        		displayIndex = i;
        	}
        	i++;
        }
        //
        // init options (merge all)
        this.fullOptions = DimensionOptionUtils.computeContextOption(dimension, null);
	}
	
	public Status initStore(String select){
		try {
			if (this.store instanceof DimensionStore){
				this.status = Status.DONE;
			}else{
				this.store.setup(this, select);
				if (this.store.isCached()) {
					this.status = Status.DONE;
				} else {
					this.status = Status.STALE;
				}
			}
		} catch (ESIndexFacadeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.status = Status.ERROR;
		}
    	return this.status;
	}
	
	
	public DimensionOption getFullOptions() {
		return fullOptions;
	}
	
	
	public void registerProxy(DimensionIndexProxy proxy){
		this.proxies.add(proxy);
	}

	
	/**
	 * check if the index should be visible
	 * @return
	 */
	public boolean isVisible() {
		return getDomain().isDynamic() || !dimension.isDynamic();
	}
	
	protected Domain getDomain() {
		return getAxis().getParent().getRoot();
	}
	
	public Project getProject() {
	    return axis.getParent().getUniverse().getProject();
	}
	
	public Status getStatus() {
	        return status;
    }
	
	public String getErrorMessage() {
        return error_message;
    }
	
	public void setDone() {
	    this.status = Status.DONE;
	    writeStatus();
	}
	
	public void setStale() {
		this.status = Status.STALE;
	    writeStatus();
	}

	/**
	 * set this index is a permanent error state with the given message information
	 * @param string
	 */
    public void setPermanentError(String message) {
        this.status = Status.ERROR;
        this.error_message = message;
	    writeStatus();
    }
    
    protected void computeDependencies() {
        List<String> dependencies = new ArrayList<>();
        // identifies the related tables        
        try {
            Universe universe = axis.getParent().getUniverse();
            ExpressionAST expr = axis.getDefinition();
            ExtractTables visitor = new ExtractTables(universe);
            List<Table> tables = visitor.apply(expr);
            for (Table table : tables) {
                dependencies.add(universe.getTableUUID(table));
            }
        } catch (ScopeException e) {
            // ignore ?
        }
    }
	
	public Axis getAxis() {
        return axis;
    }
	
	public Dimension getDimension() {
		return dimension;
	}
	
	public String getDimensionName() {
	    return dimensionName==null?dimension.getName():dimensionName;
	}
	
	public void setDimensionName(String name) {
	    this.dimensionName = name;
	}
	
	public String getDimensionPath() {
	    return dimensionPath==null?dimension.getName():dimensionPath;
	}
	
	public void setDimensionPath(String dimensionPath) {
		this.dimensionPath = dimensionPath;
	}
	
	public List<Attribute> getAttributes() {
		return attribute_cache;
	}
	
	public int getAttributeCount() {
		return attr_count;
	}
	
	protected void setParent(DimensionIndex parent) {
	    if (parent!=null) {
	        this.parent = parent;
	        parent.addChild(this);
	    }
	}

	public DimensionIndex getParent() {
		return parent;
	}
	
	protected void addChild(DimensionIndex child) {
	    if (children==null) {
	        children = new ArrayList<>();
	    }
	    children.add(child);
	}
	
	public boolean hasChildren() {
	    return children!=null && !children.isEmpty();
	}
	
	public List<DimensionIndex> getChildren() {
	    if (children==null) {
	        return Collections.emptyList();
	    } else {
	        return children;
	    }
	}
	
	/**
	 * return the root parent in the hierarchy
	 * @return the root parent, equal to this if this is the root
	 */
	public DimensionIndex getRoot() {
		DimensionIndex root = this;
		while (root.getParent()!=null) {
			root = root.getParent();
		}
		return root;
	}
	
	/**
	 * return the distance (number of levels) with the root index
	 * @return
	 */
	public int getRootDistance() {
	    int dist = 0;
        DimensionIndex root = this;
        while (root.getParent()!=null) {
            root = root.getParent();
            dist++;
        }
        return dist;
	}

	/**
	 * Always return a DimensionMember
	 * check if a member with that ID already exists and return it or else create a new one
	 * @param ID
	 * @return the DimensionMember, or a new one
	 */
	public DimensionMember getMemberByID(Object ID) {
	    return this.getStore().getMemberByID(ID);
	}

	public DimensionMember getMember(int index) {
	    return this.getStore().getMember(index);
	}

    public DimensionMember getMemberByKey(String key) {
        return this.getStore().getMemberByKey(key);
    }

    public List<DimensionMember> getMembers(int offset, int size) {
        return this.getStore().getMembers(offset,size);
    }

    public List<DimensionMember> getMembers(String filter, int offset, int size) {
        return this.getStore().getMembers(filter, offset,size);
    }
	
	public List<DimensionMember> getMembers() {
	    return this.getStore().getMembers();
	}

    public String index(List<DimensionMember> members, boolean wait) throws IndexationException{
        return this.getStore().index(members, wait);
    }

	public void index(DimensionMember member) {
		this.getStore().index(member);
	}
	
	@Deprecated
	public Collection<DimensionMember> simpleLookup(Object something) {
	    return this.getStore().getMembers(something.toString(), 0, 10);
	}

    public List<DimensionIndex> getParents() {
        if (this.parent==null) {
            return Collections.emptyList();
        } else {
            List<DimensionIndex> result = new ArrayList<>();
            DimensionIndex x = this.parent;
            while (x!=null) {
                result.add(x);
                x = x.getParent();
            }
            return result;
        }
    }
    
    public int getSize() {
        return this.getStore().getSize();
    }

    public DimensionMember index(Object[] raw) {
        return this.getStore().index(raw);
    }

    public String indexCorrelations(List<DimensionIndex> types, List<DimensionMember> values) throws IndexationException {
        return this.getStore().indexCorrelations(types,values);
    }

    public String indexCorrelations(List<DimensionIndex> types, Collection<List<DimensionMember>> batch, boolean wait) throws IndexationException {
        return this.getStore().indexCorrelations(types,batch, wait );
    }

    /**
     * initialize the hierarchy mapping
     * @param hierarchy
     */
    public void initCorrelationMapping(List<DimensionIndex> hierarchy) {
        	this.getStore().initCorrelationMapping(hierarchy);
    }

    public List<DimensionMember> getMembersFilterByParents(
            Map<DimensionIndex, List<DimensionMember>> selections, int offset, int size) {
        return this.getStore().getMembersFilterByParents(selections, offset, size);
    }

    public List<DimensionMember> getMembersFilterByParents(
            Map<DimensionIndex, List<DimensionMember>> selections, 
            String filter, int offset, int size) {
        return this.getStore().getMembersFilterByParents(selections, filter, offset, size);
    }
    
    @Override
    public String toString() {
        return "DimensionIndex:["+getDimensionName()+"]"+"="+getAxis().prettyPrint();
    }
    
    private static String DIMENSION_INDEX_REDIS_PREFIX = "DIMENSION_INDEX-" ;
    
    private void writeStatus(){
    	//logger.info( "updating index status in redis  " + this.getStatus().toString()) ;    	
    	RedisCacheProxy.getInstance().put(DIMENSION_INDEX_REDIS_PREFIX + this.getDimension().getId().toUUID() , this.getStatus().toString());
    }

    private Status readStatus(){
    	byte[] stat = RedisCacheProxy.getInstance().get(DIMENSION_INDEX_REDIS_PREFIX + this.getDimension().getId().toUUID());
    	if (stat != null)
    	{	
    		try {
    			Status status =  Status.valueOf(new String(stat));
    			logger.debug( "retrieving index '"+this+"' status from redis = " + status) ;
    			return status;
    		} catch(Exception e) {
    			logger.debug("retrieving unknown index '"+this+"' status from redis = "  + stat  + " => " + Status.STALE) ;
    			return Status.STALE;
    		}
    	}else{
    		logger.debug("could not retrieve index '"+this+"' status from redis => " + Status.STALE);
    		return Status.STALE;
    	}
    }

	public Object getDisplayName(DimensionMember member) {
		if (displayIndex>=0) {
			return member.getAttributes()[displayIndex]!=null?member.getAttributes()[displayIndex]:member.getID();
		} else {
			return member.getID();
		}
	}
	
	
	public boolean isDimensionIndexationDone(String lastIndexedDimension) {

		return this.getStore().isDimensionIndexationDone(lastIndexedDimension);
	}

	public boolean isCorrelationIndexationDone(String lastIndexedCorrelation) {
		return this.getStore().isCorrelationIndexationDone(lastIndexedCorrelation);
	}

}
