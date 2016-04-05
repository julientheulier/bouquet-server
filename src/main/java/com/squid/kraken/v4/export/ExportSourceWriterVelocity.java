package com.squid.kraken.v4.export;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.EscapeTool;
import org.apache.velocity.tools.generic.MathTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.export.IStructExportSource;
import com.squid.kraken.v4.caching.redis.datastruct.RawMatrix;
import com.squid.kraken.v4.caching.redis.datastruct.RedisCacheValuesList;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.squid.kraken.v4.model.DataTable;

public class ExportSourceWriterVelocity implements ExportSourceWriter {

	static final Logger logger = LoggerFactory
			.getLogger(ExportSourceWriterVelocity.class);

	private String templateDecoded;

	public ExportSourceWriterVelocity(String templateDecoded) {
		this.templateDecoded = templateDecoded;
	}

	@Override
	public long write(ExecuteAnalysisResult item, OutputStream outputStream) {
		try {
			logger.info("Velocity export");
			ExecutionItemStructExportSource src = new ExecutionItemStructExportSource(item);
			return this.writeStructExportSource(src, outputStream);
		} catch (ComputingException | SQLException e1) {
			logger.info(" error "  + e1.getMessage());
			return -1;
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
		DataTableStructExportSource src = new DataTableStructExportSource(matrix);
		return this.writeStructExportSource(src, out);	
	}

	private long writeStructExportSource(IStructExportSource src,
			OutputStream os) {

		VelocityEngine engine = new VelocityEngine();
		engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,
				"org.apache.velocity.runtime.log.NullLogChute");
		VelocityContext context = new VelocityContext();
		EscapeTool escapeTool = new EscapeTool();
		DateTool dateTool = new DateTool();
		MathTool mathTool = new MathTool();
		context.put("math", mathTool);
		context.put("esc", escapeTool);
		context.put("table", src);
		context.put("date", dateTool);
		context.put("tab", "\t");
		context.put("newline", "\n");
		OutputStreamWriter writer = new OutputStreamWriter(os);
		try {
			if (!engine.evaluate(context, writer, "test", templateDecoded)) {
				writeVelocityError(writer, templateDecoded, null);
			}
		} catch (Exception e) {
			logger.info("Error while evaluating the template", e);
			writeVelocityError(writer, templateDecoded, e);
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					logger.warn(e.getMessage(), e);
					return -1;
				}
			}
		}
		return 0;
	}

	private void writeVelocityError(OutputStreamWriter writer, String decoded,
			Throwable error) {
		try {
			writer.append("<html><body><p>Error while evaluating the template</p>");
			if (error != null) {
				writer.append("<p>" + error.getLocalizedMessage() + "</p>");
			}
			writer.append("<table style='border:solid 1px black;'><tr>");
			//
			writer.append("<td style='background-color:#dddddd;'><pre><code>");
			int length = decoded.split("\r\n|\r|\n").length;
			for (int line = 1; line <= length; line++) {
				writer.append(line + ".\n");
			}
			writer.append("</code></pre></td>");
			//
			writer.append("<td><pre><code>");
			writer.append(StringEscapeUtils.escapeHtml4(decoded));
			writer.append("</code></pre></td></tr></table></body></html>");
		} catch (Exception e) {
			// to bad
		}
	}

	@Override
	public long write(RedisCacheValuesList matrix, OutputStream out) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
