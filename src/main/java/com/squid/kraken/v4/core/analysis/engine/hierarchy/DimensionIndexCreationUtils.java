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
import java.util.HashMap;
import java.util.List;

import com.squid.core.domain.DomainConstant;
import com.squid.core.domain.IConstantValueDomain;
import com.squid.core.domain.IDomain;
import com.squid.core.expression.ExpressionAST;
import com.squid.core.expression.scope.ScopeException;
import com.squid.kraken.v4.core.analysis.engine.index.DimensionStoreException;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.core.analysis.universe.Axis;
import com.squid.kraken.v4.core.analysis.universe.Space;
import com.squid.kraken.v4.core.expression.visitor.ExtractOutcomes;
import com.squid.kraken.v4.model.Domain;
import com.squid.kraken.v4.model.Dimension.Type;

public class DimensionIndexCreationUtils {

    //private static final Logger logger = LoggerFactory.getLogger(DimensionIndexCreationUtils.class);

    public static DimensionIndex createConditionalIndex(DimensionIndex parent, Axis axis, IDomain type) throws InterruptedException  {
//    	logger.info("create conditional index for "+  axis.toString());

        DimensionIndexLocal local = new DimensionIndexLocal(parent, axis);
        local.getMemberByID(true);// create only one modality = TRUE
        local.setDone();
        return local;
    }
    
    public static DimensionIndex createIndex(DimensionIndex parent, Axis axis, IDomain type) throws InterruptedException, DimensionStoreException {
    	if (type.isInstanceOf(IDomain.CONDITIONAL) && parent==null) {
            return createConditionalIndex(parent, axis, type);
        } else if (type.isInstanceOf(DomainConstant.DOMAIN)) {
            // check if ok to optimize
            DimensionIndex constant = createConstantIndex(parent, axis, type);
            if (constant!=null) {
                return constant;
            }
        }
        // else
        return new DimensionIndex(parent, axis);
    }
    
    public static List<DimensionIndex> createProxyIndexes(Space space, DomainHierarchy hierarchy, Axis source) throws ComputingException, ScopeException, InterruptedException{
    	// check legacy mode
    	Domain domain = space.getDomain();
    	boolean isDomainLegacyMode = domain.getInternalVersion()==null;
    	List<DimensionIndex> result = new ArrayList<DimensionIndex>();
    	List<DimensionIndex> indexes = hierarchy.getDimensionIndexes();
        HashMap<DimensionIndex, DimensionIndex> parenting = new HashMap<>();
        //
        Space subspace = space.S(getDefinition(source));
        for (DimensionIndex index : indexes) {
        	if (index.getDimension().getType()!=Type.CONTINUOUS) {// do not proxy continuous
	            // create the axis that traverse the main dimension to join the sub-domain
	            Axis relink = subspace.A(index.getAxis());
	            // define the index as a proxy to the sub-domain
	            DimensionIndex new_parent = index.getParent()!=null?parenting.get(index.getParent()):null;
	            DimensionIndex proxy = new DimensionIndexProxy(source, new_parent, relink, index);
	            // - default is to concatenate source dimension name and sub dimension name
	            // - but you can bypass that logic by prefixing the source dimension name with underscore (krkn-110)
	            if (!isDomainLegacyMode && source.getDimension().getName().startsWith("__")) {
	            	proxy.setDimensionName(index.getDimension().getName());
	            } else if (isDomainLegacyMode && source.getDimension().getName().startsWith("_")) {
	            	proxy.setDimensionName(index.getDimension().getName());
	            } else if (source.getDimension().getName().startsWith("_")) {
	            	proxy.setDimensionName(index.getDimensionName());
	            } else {
	            	// default is to concat the subdomain with the target name
	            	proxy.setDimensionName(source.getDimension().getName() + " " + index.getDimensionName());
	            	proxy.setCompositeName(true);// it's composite - UI can manage it independently
	            }
	            // update the path
	            if (index.getDimensionPath().equals("")) {
	            	proxy.setDimensionPath(getCleanName(source.getDimension().getName()));
	            } else {
	            	proxy.setDimensionPath(index.getDimensionPath()+"/"+getCleanName(source.getDimension().getName()));
	            }
	            parenting.put(index, proxy);
	 //           logger.info("adding proxy " + proxy.toString());
	            result.add(proxy);
        	}
        }
        return result;
    }
    
    private static String getCleanName(String dimensionName) {
    	if (dimensionName.startsWith(">")) {
    		return dimensionName.substring(1);
    	} else if (dimensionName.startsWith("__")) {
    		return dimensionName.substring(2);
    	} else if (dimensionName.startsWith("_")) {
    		return dimensionName.substring(1);
    	} else {
    		return dimensionName;
    	}
    }
    
    public static DimensionIndex createConstantIndex(DimensionIndex parent, Axis axis, IDomain type)  {
//    	logger.info("create constant index for " +axis.toString());

        // check if ok to optimize
        try {
            if (parent==null && axis.H().isEmpty()) {
                // this is a constant expression with no correlations
                // we can statically evaluate the list of outputs
                ExtractOutcomes extract = new ExtractOutcomes();
                ExpressionAST definition = getDefinition(axis);
                List<ExpressionAST> outcomes = extract.apply(definition);
                ArrayList<String> values = new ArrayList<>();
                for (ExpressionAST outcome : outcomes) {
                    IDomain outcomeType = outcome.getImageDomain();
                    if (outcomeType instanceof IConstantValueDomain) {
                    	IConstantValueDomain<?> value = (IConstantValueDomain<?>)outcomeType;
                        if (value.getValue()!=null) {
                            values.add(value.getValue().toString());
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                }
                // ok, create the DimensionIndex
                DimensionIndexLocal local = new DimensionIndexLocal(parent, axis);
                for (String value : values) {
                    local.getMemberByID(value);
                }
                local.setDone();
                return local;
            }
        } catch (Exception e) {
            // ignore
        }
        // else
        return null;
    }
    
    
    public static DimensionIndex createInvalidIndex(DimensionIndex parent, Axis axis, String message) throws InterruptedException {
//        logger.error("cannot build index for " + axis.toString() + ": " + message);
        DimensionIndex index = new DimensionIndexLocal(parent, axis);
        index.setPermanentError(message);
        return index;
    }

    public static ExpressionAST getDefinition(Axis axis) throws ScopeException {
        if (axis==null) throw new ScopeException("undefined axis");
    	return axis.getDefinition();
    }
}
