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
package com.squid.kraken.v4.export;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.model.DataTable;

public class ExportSourceWriterText implements ExportSourceWriter {
	
	private static final Logger logger = LoggerFactory
			.getLogger(ExportSourceWriterText.class);

	@Override
	public long write(ExecuteAnalysisResult item, OutputStream out) {
		try {
			ResultSet rs = item.getItem().getResultSet();
			ResultSetMetaData metadata = rs.getMetaData();
			int columnCount = metadata.getColumnCount();
			int lineCnt = 0;
			Object[] nextLine;
			while (rs.next()) {
				nextLine = new Object[columnCount];
				lineCnt++;
				for (int i = 0; i < columnCount; i++) {
					nextLine[i] = rs.getObject(i + 1);
				}
				writeLine(nextLine, out);
			}
			logger.info("lines written : "+lineCnt);
			return lineCnt;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void writeLine(Object[] nextLine, OutputStream out) {
		StringBuilder b = new StringBuilder();
		String sep = null;
		for (Object o : nextLine) {
			if (sep == null) {
				sep = ",";
			} else {
				b.append(sep);
			}
			if (o != null) {
				b.append(o.toString());
			} else {
				b.append("NULL");
			}
			
		}
		b.append('\n');
		try {
			out.write(b.toString().getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long write(RawMatrix matrix, OutputStream out) {
        throw new UnsupportedOperationException();					
    }

	@Override
	public long write(DataMatrix matrix, OutputStream out) {
        throw new UnsupportedOperationException();					
    }

	@Override
	public long write(DataTable matrix, OutputStream out) {
        throw new UnsupportedOperationException();				
    }

	@Override
	public long write(RedisCacheValuesList matrix, OutputStream out) {
        throw new UnsupportedOperationException();			
	}

}
