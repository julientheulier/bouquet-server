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
package com.squid.kraken.v4.model.visitor;

import java.util.List;

import com.squid.kraken.v4.model.Dimension;
import com.squid.kraken.v4.model.Persistent;
import com.squid.kraken.v4.model.Project;
import com.squid.kraken.v4.model.ProjectAnalysisJob;
import com.squid.kraken.v4.persistence.AppContext;
import com.squid.kraken.v4.persistence.DAOFactory;
import com.squid.kraken.v4.persistence.dao.DimensionDAO;
import com.squid.kraken.v4.persistence.dao.ProjectAnalysisJobDAO;

/**
 * A ModelVisitor which handles cascade delete.
 */
public class DeletionVisitor extends ReverseModelVisitor {

	public DeletionVisitor(AppContext ctx) {
		super(ctx);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void visitElement(Persistent<?> object) {
		if (object instanceof Dimension) {
			visitInstance((Dimension) object);
		} else if (object instanceof Project) {
			visitInstance((Project) object);
		}
		DAOFactory.getDAOFactory().getDAO(object.getClass())
				.delete(ctx, object.getId());
	}

	private void visitInstance(Dimension object) {
		// delete sub-dimensions
		List<Dimension> subdimensions = ((DimensionDAO) DAOFactory
				.getDAOFactory().getDAO(Dimension.class)).findByParent(ctx,
				object);
		for (Dimension dim : subdimensions) {
			DAOFactory.getDAOFactory().getDAO(dim.getClass())
					.delete(ctx, dim.getId());
		}
	}

	private void visitInstance(Project object) {
		List<ProjectAnalysisJob> analysisJobs = ((ProjectAnalysisJobDAO) DAOFactory
				.getDAOFactory().getDAO(ProjectAnalysisJob.class))
				.findByProject(ctx, object.getId());
		for (ProjectAnalysisJob o : analysisJobs) {
			DAOFactory.getDAOFactory().getDAO(o.getClass())
					.delete(ctx, o.getId());
		}
	}

}
