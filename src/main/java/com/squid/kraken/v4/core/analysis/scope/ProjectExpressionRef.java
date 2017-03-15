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
package com.squid.kraken.v4.core.analysis.scope;

import com.squid.core.domain.IDomain;
import com.squid.core.domain.operators.ExtendedType;
import com.squid.core.expression.ExpressionRef;
import com.squid.core.expression.scope.IdentifierType;
import com.squid.core.sql.render.SQLSkin;
import com.squid.kraken.v4.core.model.domain.DomainDomain;
import com.squid.kraken.v4.model.Project;

/**
 * @author sergefantino
 *
 */
public class ProjectExpressionRef extends ExpressionRef {
	
	private Project project;
	
	public ProjectExpressionRef(Project project) {
		super();
		this.project = project;
	}

	/* (non-Javadoc)
	 * @see com.squid.core.expression.ExpressionAST#computeType(com.squid.core.sql.render.SQLSkin)
	 */
	@Override
	public ExtendedType computeType(SQLSkin skin) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.squid.core.expression.ExpressionAST#getImageDomain()
	 */
	@Override
	public IDomain getImageDomain() {
		return DomainDomain.DOMAIN;
	}

	/* (non-Javadoc)
	 * @see com.squid.core.expression.ExpressionAST#getSourceDomain()
	 */
	@Override
	public IDomain getSourceDomain() {
		return IDomain.NULL;
	}

	/* (non-Javadoc)
	 * @see com.squid.core.expression.ExpressionRef#getReference()
	 */
	@Override
	public Object getReference() {
		return project;
	}
	
	public Project getProject() {
		return project;
	}

	@Override
	public String getReferenceName() {
		return project.getName();
	}

	@Override
	public String getReferenceIdentifier() {
		return project.getId().getProjectId();
	}
	
	@Override
	public IdentifierType getReferenceType() {
		return null;
	}

}
