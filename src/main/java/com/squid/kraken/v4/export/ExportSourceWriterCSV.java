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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.csv.CSVSettingsBean;
import com.squid.core.csv.CSVWriter;
import com.squid.core.export.IRawExportSource;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.model.DataTable;

public class ExportSourceWriterCSV implements ExportSourceWriter {

	private static final Logger logger = LoggerFactory
			.getLogger(ExportSourceWriterCSV.class);

	private static final int SPLIT_SIZE = 0;
	
	private CSVSettingsBean settings = null;
	
	public ExportSourceWriterCSV() {
		settings = new CSVSettingsBean();
		settings.setSeparator(',');
	}
	
	public CSVSettingsBean getSettings() {
		return settings;
	}
	
	public void setSettings(CSVSettingsBean settings) {
		this.settings = settings;
	}

	@Override
	public long write(ExecuteAnalysisResult item, OutputStream out) {
		Writer output = null;
		CSVWriter writer;
		IVendorSupport vendorSpecific;
		vendorSpecific = VendorSupportRegistry.INSTANCE
				.getVendorSupport(item.getItem().getDatabase());
		writer = new CSVWriter(settings);
		
		ResultSet rs = item.getItem().getResultSet();
		try {
			IRawExportSource source = new ExecutionItemExportSource(item, settings);

			IJDBCDataFormatter formatter = vendorSpecific.createFormatter(
					settings, rs.getStatement().getConnection());
			output   = new OutputStreamWriter(out);
			writer.writeResultSet(output, SPLIT_SIZE, source, formatter);
			output.flush();
		} catch (SQLException e) {
			logger.warn(e.getMessage(), e);
		} catch (IOException e) {
			logger.warn(e.getMessage(), e);
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					logger.warn(e.getMessage(), e);
				}
			}
		}
		
		return writer.getLinesWritten();
	}
	
	@Override
	public long write(RawMatrix  matrix, OutputStream out) {
		Writer output = null;
		CSVWriter writer;
		writer = new CSVWriter(settings);
		IRawExportSource source =  new RawMatrixExportSource(matrix);
		try {
			
			IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(null);
			IJDBCDataFormatter formatter = vendorSpecific.createFormatter(settings, null);
			
			output = new OutputStreamWriter(out);
			writer.writeResultSet(output, SPLIT_SIZE, source	, formatter);
			output.flush();
		} catch (SQLException e) {
			logger.warn(e.getMessage(), e);
		} catch (IOException e) {
			logger.warn(e.getMessage(), e);
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					logger.warn(e.getMessage(), e);
				}
			}
		}
		
		return writer.getLinesWritten();
	}
	
	@Override
	public long write(DataMatrix  matrix, OutputStream out) {
		Writer output = null;
		CSVWriter writer;
		writer = new CSVWriter(settings);
		IRawExportSource source =  new DataMatrixExportSource(matrix);
		try {
			
			IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(null);
			IJDBCDataFormatter formatter = vendorSpecific.createFormatter(settings, null);
			
			output = new OutputStreamWriter(out);
			writer.writeResultSet(output, SPLIT_SIZE, source	, formatter);
			output.flush();
		} catch (SQLException e) {
			logger.warn(e.getMessage(), e);
		} catch (IOException e) {
			logger.warn(e.getMessage(), e);
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					logger.warn(e.getMessage(), e);
				}
			}
		}
		
		return writer.getLinesWritten();
	}

	@Override
	public long write(DataTable matrix, OutputStream out) {
        throw new UnsupportedOperationException();				
	}
	

}
