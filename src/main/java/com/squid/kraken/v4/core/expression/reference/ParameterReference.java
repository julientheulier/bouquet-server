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
package com.squid.kraken.v4.core.expression.reference;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.PrettyPrintConstant;
import com.squid.core.sql.render.SQLSkin;


public class ParameterReference extends ExpressionRef {
	
	private String parameterName;
	private IDomain image;
	
	public ParameterReference(String parameterName, IDomain image) {
		super();
		this.parameterName = parameterName;
		this.image = image;
	}
	
	public String getParameterName() {
		return parameterName;
	}

	@Override
	public ExtendedType computeType(SQLSkin skin) {
		return image.computeType(skin);
	}

	@Override
	public IDomain getImageDomain() {
		return image;
	}

	@Override
	public IDomain getSourceDomain() {
		return IDomain.NULL;
	}

	@Override
	public String getReferenceName() {
		return parameterName;
	}
	
	@Override
	public Object getReference() {
		return parameterName;
	}
	
	@Override
	public String getReferenceIdentifier() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public String prettyPrint() {
		return PrettyPrintConstant.PARAMETER_TAG+PrettyPrintConstant.OPEN_IDENT+parameterName+PrettyPrintConstant.CLOSE_IDENT;
	}

}
