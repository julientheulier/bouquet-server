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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.csv.CSVSettingsBean;
import com.squid.core.csv.CSVWriter;
import com.squid.core.export.IRawExportSource;
import com.squid.core.export.Selection;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.DataTable;

public class ExportSourceWriterCSV implements ExportSelectionWriter {

	private static final Logger logger = LoggerFactory.getLogger(ExportSourceWriterCSV.class);

	private static final int SPLIT_SIZE = 0;

	private CSVSettingsBean settings = null;
	private List<Selection> selection = null;

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
	public void setSelection(List<Selection> selection) {
		this.selection = selection;
	}

	@Override
	public long write(ExecuteAnalysisResult item, OutputStream out) {
		Writer output = null;
		CSVWriter writer;
		IVendorSupport vendorSpecific;
		vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(item.getItem().getDatabase());
		writer = new CSVWriter(settings);
		writer.setSelection(selection);

		ResultSet rs = item.getItem().getResultSet();
		try {
			IRawExportSource source = new ExecutionItemExportSource(item);

			IJDBCDataFormatter formatter = vendorSpecific.createFormatter(settings, rs.getStatement().getConnection());
			output = new OutputStreamWriter(out);
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
	public long write(RawMatrix matrix, OutputStream out) {
		IRawExportSource source = new RawMatrixExportSource(matrix);
		return this.write(source, out, null);
	}

	@Override
	public long write(DataMatrix matrix, OutputStream out) {
		IRawExportSource source = new DataMatrixExportSource(matrix);
		return this.write(source, out, null);
	}

	@Override
	public long write(DataTable matrix, OutputStream out) {
		throw new UnsupportedOperationException();
	}

	@Override
	public long write(RedisCacheValuesList matrix, OutputStream out) throws ComputingException {
		IRawExportSource source;
		try {
			source = new ChunkedRawMatrixExportSource(matrix);
			return this.write(source, out, null);

		} catch (InterruptedException | ExecutionException e) {
			throw new ComputingException();
		}

	}

	private long write(IRawExportSource source, OutputStream out, Connection connection) {
		Writer output = null;
		CSVWriter writer;
		writer = new CSVWriter(settings);
		writer.setSelection(selection);
		try {

			IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(null);
			IJDBCDataFormatter formatter = vendorSpecific.createFormatter(settings, connection);

			output = new OutputStreamWriter(out);
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

}
