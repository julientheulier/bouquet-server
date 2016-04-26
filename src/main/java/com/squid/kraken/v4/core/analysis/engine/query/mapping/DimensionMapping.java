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
package com.squid.kraken.v4.core.analysis.engine.query.mapping;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.squid.core.sql.render.ISelectPiece;
import com.squid.kraken.v4.core.analysis.datamatrix.AxisValues;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionIndex;
import com.squid.kraken.v4.model.Attribute;
import com.squid.kraken.v4.model.Domain;

public class DimensionMapping extends MultiMapping {
	
    private Domain domain;
	private DimensionIndex index;
	private AxisValues axisData;

    private static final int COMPUTE_OPTION = 1;
	public static final int COMPUTE_INDEX = COMPUTE_OPTION;
    public static final int COMPUTE_CORRELATIONS = COMPUTE_OPTION<<1;
	
	private int option = COMPUTE_INDEX | COMPUTE_CORRELATIONS;

	public DimensionMapping(ISelectPiece piece, Domain domain, DimensionIndex index) {
		super(piece);
		this.index = index;
		this.domain = domain;
	}
	
	public int getOption() {
        return option;
    }
	
	public void setOption(int option) {
        this.option = option;
    }

    public boolean isOption(int option) {
        return (this.option & option)==option;
    }

	public DimensionIndex getDimensionIndex() {
		return index;
	}

	public void setAxisData(AxisValues axisData) {
		this.axisData = axisData;
	}

	public AxisValues getAxisData() {
		return axisData;
	}
	
	@Override
	public void setMetadata(ResultSet result, ResultSetMetaData metadata) throws SQLException {
		super.setMetadata(result, metadata);
		// take care of the attributes
		for (Attribute attr : getDimensionIndex().getAttributes()) {
			SimpleMapping s = getMapping(attr.getId().getAttributeId());
            s.setMetadata(result, metadata);
		}
	}

    public Domain getDomain() {
        return domain;
    }
	
	
}