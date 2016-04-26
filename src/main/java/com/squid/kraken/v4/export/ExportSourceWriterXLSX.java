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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.export.IRawExportSource;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.core.poi.ExcelSettingsBean;
import com.squid.core.poi.ExcelWriter;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.DataTable;

public class ExportSourceWriterXLSX implements ExportSourceWriter {

  private static final Logger logger = LoggerFactory.getLogger(ExportSourceWriterXLSX.class);

  private ExcelSettingsBean settings = null;

  public ExportSourceWriterXLSX() {
    settings = new ExcelSettingsBean();
  }

  public ExportSourceWriterXLSX(ExcelSettingsBean settings) {
    this.settings = settings;
  }

  public ExcelSettingsBean getSettings() {
    return settings;
  }

  public void setSettings(ExcelSettingsBean settings) {
    this.settings = settings;
  }

  @Override
  public long write(ExecuteAnalysisResult item, OutputStream out) {
    ExcelWriter writer;
    IVendorSupport vendorSpecific;
    vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(item.getItem().getDatabase());
    writer = new ExcelWriter(out, settings);

    ResultSet rs = item.getItem().getResultSet();
    try {
      IRawExportSource source = new ExecutionItemExportSource(item);

      IJDBCDataFormatter formatter = vendorSpecific.createFormatter(settings, rs.getStatement().getConnection());
      writer.writeResultSet(source, formatter);
      writer.flush();
    } catch (SQLException e) {
      logger.warn(e.getMessage(), e);
    } catch (IOException e) {
      logger.warn(e.getMessage(), e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
          writer.dispose();
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
    ExcelWriter writer = new ExcelWriter(out, settings);
    ;
    try {

      IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(null);
      IJDBCDataFormatter formatter = vendorSpecific.createFormatter(settings, connection);

      writer.writeResultSet(source, formatter);
      writer.flush();
    } catch (SQLException e) {
      logger.warn(e.getMessage(), e);
    } catch (IOException e) {
      logger.warn(e.getMessage(), e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
          writer.dispose();
        } catch (IOException e) {
          logger.warn(e.getMessage(), e);
        }
      }
    }

    return writer.getLinesWritten();

  }

}
