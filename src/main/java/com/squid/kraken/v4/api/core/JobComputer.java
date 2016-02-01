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
package com.squid.kraken.v4.api.core;

import java.io.OutputStream;

import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.export.ExportSourceWriter;
import com.squid.kraken.v4.model.ComputationJob;
import com.squid.kraken.v4.model.GenericPK;
import com.squid.kraken.v4.persistence.AppContext;

/**
 * A class which knows how to compute a Job.
 * 
 * @param <T>
 *            ComputationJob type
 * @param <PK>
 *            ComputationJob PK type
 */
public interface JobComputer<T extends ComputationJob<PK, R>, PK extends GenericPK, R extends JobResult> {

	public R compute(AppContext ctx, T job, Integer maxResults, Integer startIndex, boolean lazy) throws ComputingException,
			InterruptedException;

	public R compute(AppContext ctx, T job, OutputStream outputStream,
			ExportSourceWriter writer, boolean lazy) throws ComputingException,
			InterruptedException;

}
