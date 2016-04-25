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
package com.squid.kraken.v4.caching.redis.datastruct;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.squid.kraken.v4.api.core.PerfDB;
import com.squid.kraken.v4.api.core.SQLStats;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.squid.core.expression.scope.ScopeException;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.core.analysis.engine.hierarchy.DimensionValuesDictionary;
import com.squid.kraken.v4.core.analysis.engine.processor.ComputingException;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;

public class RawMatrix extends RedisCacheValue {

	private transient ArrayList<RawRow> rows;
	private transient ArrayList<Integer> colTypes;
	private transient ArrayList<String> colNames;
	private transient boolean moreData;

	private transient boolean fromCache = false;
	private transient Date executionDate = new Date();

	// used to deserialize /compatibility with older version
	private transient int version = VERSION;
	private transient HashMap<String, Integer> registration;

	private transient static int maxChunkSizeInMB = 50;
		
	static final Logger logger = LoggerFactory.getLogger(RawMatrix.class);

	public RawMatrix() {
		this.rows = new ArrayList<RawRow>();
		this.colTypes = new ArrayList<Integer>();
		this.colNames = new ArrayList<String>();
		this.moreData = true;
	}

	public RawMatrix(int v, HashMap<String, Integer> registration) {
		this();
		this.version = v;
		this.registration = registration;
	}

	public static RawMatrix getTestMatrix() {
		RawMatrix test = new RawMatrix();

		test.colNames.add("string column");
		test.colNames.add("int column");

		test.colTypes.add(java.sql.Types.VARCHAR);
		test.colTypes.add(java.sql.Types.INTEGER);

		int i = 0;

		// init row
		while (i < 10) {
			Object[] r = new Object[2];
			test.addRow(new RawRow(r));
			i++;
		}
		i = 0;
		while (i < 10) {
			test.rows.get(i).data[0] = new String("abc" + i);
			test.rows.get(i).data[1] = new Integer(i);
			i++;
		}
		return test;
	}

	public boolean isFromCache() {
		return fromCache;
	}

	public void setFromCache(boolean fromCache) {
		this.fromCache = fromCache;
	}

	public Date getExecutionDate() {
		return executionDate;
	}

	public void setExecutionDate(Date executionDate) {
		this.executionDate = executionDate;
	}

	public ArrayList<RawRow> getRows() {
		return rows;
	}

	public ArrayList<String> getColNames() {
		return this.colNames;
	}

	public ArrayList<Integer> getColTypes() {
		return this.colTypes;
	}

	public void addRow(RawRow e) {
		this.rows.add(e);
	}

	public boolean hasMoreData() {
		return this.moreData;
	}

	public boolean isMoreData() {
		return moreData;
	}

	public void setMoreData(boolean moreData) {
		this.moreData = moreData;
	}

	public void setColTypes(ArrayList<Integer> colTypes) {
		this.colTypes = colTypes;
	}

	public void setColNames(ArrayList<String> colNames) {
		this.colNames = colNames;
	}
	
	public static void setMaxChunkSizeInMB(int size){
		maxChunkSizeInMB = size;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (!(obj instanceof RawMatrix))
			return false;
		RawMatrix m = (RawMatrix) obj;
		if (m.moreData != this.moreData)
			return false;
		if (m.rows.size() != this.rows.size())
			return false;
		if (m.colNames.size() != this.colNames.size())
			return false;

		for (int j = 0; j < this.colNames.size(); j++) {
			if (!m.colNames.get(j).equals(this.colNames.get(j)))
				return false;
			if (!m.colTypes.get(j).equals(this.colTypes.get(j)))
				return false;
		}

		for (int i = 0; i < this.rows.size(); i++) {
			if (!(this.rows.get(i).equals(m.rows.get(i))))
				return false;
		}
		return true;
	}

	@Override
	public String toString() {
		String res = "";
		for (String n : this.colNames)
			res += n + "\t";
		res += "\n";
		for (Integer t : this.colTypes)
			res += t + "\t";
		res += "\n";
		if (this.moreData)
			res += "more data\n";
		else
			res += "no more data\n";

		for (RawRow r : this.rows)
			res += r.toString() + "\n";
		return res;
	}

	public static RawMatrix readExecutionItem(IExecutionItem item, long maxRecords)
			throws SQLException, ScopeException {
		long metter_start = System.currentTimeMillis();
		try {
			RawMatrix matrix = new RawMatrix();
			ResultSet result = item.getResultSet();
			IJDBCDataFormatter formatter = item.getDataFormatter();
			//
			ResultSetMetaData metadata = result.getMetaData();
			int nbColumns = metadata.getColumnCount();

			IVendorSupport vendorSpecific;
			vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(item.getDatabase());

			int[] normalizedTypes = vendorSpecific.getVendorMetadataSupport().normalizeColumnType(result);

			int i = 0;
			while (i < nbColumns) {
				matrix.colTypes.add(normalizedTypes[i]);
				matrix.colNames.add(metadata.getColumnName(i + 1));
				i++;
			}

			int count = 0;
			matrix.moreData = false;
			//
			while ((count++ < maxRecords || maxRecords < 0) && (matrix.moreData = result.next())) {
				Object[] rawrow = new Object[nbColumns];

				i = 0;
				while (i < nbColumns) {
					Object value = result.getObject(i + 1);
					Object unbox = formatter.unboxJDBCObject(value, matrix.colTypes.get(i));
					if (unbox instanceof String) {
						String stringVal = (String) unbox;
						rawrow[i] = DimensionValuesDictionary.INSTANCE.getRef(stringVal);
					} else {
						rawrow[i] = unbox;
					}
					i++;
				}
				matrix.addRow(new RawRow(rawrow));
				count++;
			}
			long metter_finish = new Date().getTime();
			if (logger.isDebugEnabled()) {
				logger.debug(("SQLQuery#" + item.getID() + "read " + (count - 1) + " row(s) in "
						+ (metter_finish - metter_start) + " ms."));
			}
			return matrix;
		} finally {
			item.close();
		}
	}

	/**
	 * Serialization format
	 * 
	 * 
	 * - kryo registration # of classes registered to kryo (int) list of full
	 * classe name(String)/registration ID(Integer)
	 *
	 * - RedisCacheValue.VERSION - RedisCacheValue.RedisCacheType.RAW_MATRIX
	 * 
	 * - number of columns (Int)
	 * 
	 * - the column names (String list)
	 * 
	 * - the column type (int list)
	 * 
	 * - for each serialized row, 3 a boolean flag set to true (so that the
	 * deserialize still more rows if the object to be serialized is a String,
	 * we try to optimize if this is the first occurrence of this string value,
	 * we associated an id to it a flag indicating that this is a first time
	 * encountered value the serialized String else a flag indicating that this
	 * is a reference to a previously encountered String (int) the id (int) else
	 * a flag indicating that its not a string value (int) the serialized object
	 * 
	 * - a boolean set to false (no more rows)
	 * 
	 * - a boolean indicating if there are more rows to be read from the result
	 * set
	 */

	private static final int MEMBER_REFERENCE = 1;// this is reference in the
	// lookup table
	private static final int MEMBER_DEFINITION = 2;// this is a definition
	// (value+future reference)
	private static final int MEMBER_VALUE = 3;// this is a simple value

	public static RawMatrixStreamExecRes streamExecutionItemToByteArray(IExecutionItem item, long maxRecords,
			long nbLinesExpectedLeft) throws IOException, SQLException {

		RawMatrixStreamExecRes res = new RawMatrixStreamExecRes();
		long metter_start = System.currentTimeMillis();
		ByteArrayOutputStream baout = new ByteArrayOutputStream();
		Output kout = new Output(baout);

		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(true);
		kryo.setReferences(false);

		ResultSet result = item.getResultSet();
		IJDBCDataFormatter formatter = item.getDataFormatter();
		//
		// if(logger.isDebugEnabled()){logger.debug(("Formatter
		// ="+formatter.getClass().toString()));}

		ResultSetMetaData metadata = result.getMetaData();
		int nbColumns = metadata.getColumnCount();

		IVendorSupport vendorSpecific = VendorSupportRegistry.INSTANCE.getVendorSupport(item.getDatabase());

		int[] colTypes = vendorSpecific.getVendorMetadataSupport().normalizeColumnType(result);

		// get columns #, type and names
		String[] colNames = new String[nbColumns];
		int i = 0;
		while (i < nbColumns) {
			colNames[i] = metadata.getColumnName(i + 1);
			i++;
		}
		// register
		// Class mapping have to be registered before we start writing
		HashMap<String, Integer> registration = new HashMap<String, Integer>();
		for (int val : colTypes) {
			if (!isPrimitiveType(val)) {
				String className = getJavaDatatype(val);
				try {
					if (registration.get(className) == null) {
						registration.put(className, kryo.register(Class.forName(className)).getId());
					}
				} catch (ClassNotFoundException e0) {
					logger.info("Class " + className + " not found");
				} catch (NullPointerException e1) {
					logger.info("Class " + className + " not found");
				}
			}
		}

		// Register Hadoop type
		// registration.put("org.apache.hadoop.io.Text",kryo.register(org.apache.hadoop.io.Text.class).getId());
		// registration.put("byte[]", kryo.register(byte[].class).getId());

		// start writing!

		// registration
		kout.writeInt(registration.keySet().size());
		for (String s : registration.keySet()) {
			kout.writeString(s);
			kout.writeInt(registration.get(s));
			// logger.info(s + " " + registration.get(s));
		}

		// version
		int version = VERSION;
		if (version >= 1) {
			kout.writeInt(-1);// this is for V0 compatibility which miss
			// version information
			kout.writeInt(version);
		}

		// Redis cache type
		kout.writeInt(RedisCacheType.RAW_MATRIX.ordinal());

		// nb of columns
		kout.writeInt(nbColumns);

		// columns names
		for (String n : colNames)
			kout.writeString(n);

		// column type
		for (Integer t : colTypes)
			kout.writeInt(t);

		// rows
		// we need a different dictionary to check for first occurences
		HashMap<String, Integer> tempsDict = new HashMap<String, Integer>();
		int count = 0;
		int index = 0;
		boolean moreData = false;
		boolean maxSizeReached = false;
		// while ((count<maxRecords || maxRecords<0) && (moreData =
		// result.next())) {

		while ((!maxSizeReached) && (moreData = result.next())) {
			i = 0;
			kout.writeBoolean(true);
			while (i < nbColumns) {
				Object value = result.getObject(i + 1);
				Object unbox = formatter.unboxJDBCObject(value, colTypes[i]);
				// if(logger.isDebugEnabled()){logger.debug(("unbox value is
				// "+unbox));}
				if (unbox instanceof String) {
					String stringVal = (String) unbox;
					// System.out.println(stringVal);
					Integer ref = tempsDict.get(stringVal);
					if (ref != null) {
						kout.write(MEMBER_REFERENCE);// 4
						kout.writeInt(ref);// 5
					} else {
						kout.write(MEMBER_DEFINITION);// 4
						kout.writeString(stringVal);
						tempsDict.put(stringVal, new Integer(index));
						index++;
					}
				} else {
					kout.write(MEMBER_VALUE);// 4
					// if(logger.isDebugEnabled()){logger.debug(("member
					// unbox " + unbox.toString()));}
					// if(logger.isDebugEnabled()){logger.debug(("member
					// value " + value.toString()));}
					kryo.writeClassAndObject(kout, unbox);
				}
				i++;
			}

			count++;

			// stats: display time for first 100th rows
			if (count == 100) {
				long intermediate = new Date().getTime();
				// logger.info("SQLQuery#" + item.getID() + " proceeded
				// first 100 items in "+(intermediate-metter_start)+" ms");
				logger.info("task=RawMatrix" + " method=streamExecutionItemToByteArray" + " duration="
						+ ((intermediate - metter_start)) + " error=false status=running queryid=" + item.getID());

			}
			// if max chunk size of 50MB reached, stop 
			if(count % 100 ==0){	
				float size = Math.round(baout.size() / 1048576);
				if (size >= maxChunkSizeInMB) {
					logger.info("Max size of " + maxChunkSizeInMB+ "MB for one chunk reached");
					maxSizeReached = true;
				}
			}

			// DEBUG CODE TO CREATE SMALLER CHUNKS
	/*		if (count == 250){
				maxSizeReached= true;
//				logger.info("Max debug size of 250 items reached");
			}  */
		}

		// WRITE moredata
		kout.writeBoolean(false);// close the data stream, begin metadata
		
		
		//we stop either if maxSize was reach or if there were no more data to read
		boolean moreToRead = false; // we did not reach the end of the resultset
		boolean moreThanLimit=false; // is case of a limit query, did we reach the limit
		if (maxSizeReached){ // we  stopped because reached the hard memory limit for one chunk
			if ( (nbLinesExpectedLeft>-1) && ( !(count < nbLinesExpectedLeft))){ //we read exqctly as many lines as the limit
				moreThanLimit =  true;	
			}else{
				moreThanLimit = true  ;					
				moreToRead = true;
			}
		}else{ 
			if ( !moreData){ //no more lines to read
				if(nbLinesExpectedLeft>-1){ // limit
					if ( !(count < nbLinesExpectedLeft)){ //we read as many lines as the limit
						moreThanLimit = true ;	
					}
				}
			}
		}
		
		kout.writeBoolean(moreThanLimit);
		// -- V1 only
		if (version >= 1) {
			kout.writeLong(item.getExecutionDate().getTime());// the
			// computeDate
		}

		// stats: display total
		long metter_finish = new Date().getTime();
		// logger.info("SQLQuery#" + item.getID() + " serialized
		// "+(count-1)+" row(s) in "+(metter_finish-metter_start)+" ms,
		// compressed resulset size is "+size+" Mbytes");
	/*	logger.info("task=RawMatrix" + " method=streamExecutionItemToByteArray" + " duration="
				+ (metter_finish - metter_start) / 1000 + " error=false status=done driver="
				+ item.getDatabase().getName() + " queryid=" + item.getID() + " size= " + size + " SQLQuery#"
				+ item.getID() + " serialized " + (count - 1) + " row(s) in " + (metter_finish - metter_start)
				+ " ms, compressed resulset size is " + size + " Mbytes"); */
		// TODO Get project
		SQLStats queryLog = new SQLStats(Integer.toString(item.getID()), "streamExecutionItemToByteArray", "",
				(metter_finish - metter_start), item.getDatabase().getName());
		queryLog.setError(false);
		PerfDB.INSTANCE.save(queryLog);

		kout.close();

		res.setHasMore(moreToRead);			
		res.setExecutionTime(metter_finish - metter_start);
		res.setNbLines(count);
		res.setStreamedMatrix(baout.toByteArray());

		return res;

	}

	private void writeObject(Output out) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug((" in write object"));
		}
		long start = new Date().getTime();

		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(true);
		kryo.setReferences(false);

		HashMap<String, Integer> registration = new HashMap<String, Integer>();

		// Class mapping have to be registered before we start writing
		for (Integer t : this.colTypes) {
			int val = t.intValue();
			if (!isPrimitiveType(val)) {
				String className = getJavaDatatype(val);
				try {
					if (registration.get(className) == null) {
						registration.put(className, kryo.register(Class.forName(className)).getId());
					}
				} catch (ClassNotFoundException e0) {
					logger.info("Class " + className + " not found");
				} catch (NullPointerException e1) {
					logger.info("Class " + className + " not found");
				}
			}
		}

		// registration.put("org.apache.hadoop.io.Text",kryo.register(org.apache.hadoop.io.Text.class).getId());
		// registration.put("byte[]", kryo.register(byte[].class).getId());

		// WRITE class mapping
		out.writeInt(registration.keySet().size());
		for (String s : registration.keySet()) {
			out.writeString(s);
			out.writeInt(registration.get(s));
		}

		// WRITE version
		int version = VERSION;
		if (version >= 1) {
			out.writeInt(-1);
			out.writeInt(version);
		}

		// WRITE RedisCacheValue.RedisCacheType.RAW_MATRIX
		out.writeInt(RedisCacheType.RAW_MATRIX.ordinal());

		// WRITE nb columns
		out.writeInt(this.colNames.size());

		// WRITE column names, columns size
		for (String n : this.colNames)
			out.writeString(n);
		for (Integer t : this.colTypes)
			out.writeInt(t);

		// WRITE data

		// we need a different dictionary to check for first occurences
		HashMap<String, Integer> tempsDict = new HashMap<String, Integer>();

		int count = 0;
		for (RawRow row : this.rows) {
			out.writeBoolean(true);
			for (Object value : row.data) {
				if (value instanceof String) {
					String svalue = (String) value;
					Integer ref = tempsDict.get(svalue);
					if (ref != null) {
						out.write(MEMBER_REFERENCE);// 4
						out.writeInt(ref);// 5
					} else {
						out.write(MEMBER_DEFINITION);// 4
						out.writeString(svalue);
						tempsDict.put(svalue, new Integer(count));
						count++;
					}
				} else {
					out.write(MEMBER_VALUE);// 4
					kryo.writeClassAndObject(out, value);
				}
			}
		}

		// WRITE more data to be fetch
		out.writeBoolean(false);// close the data stream, begin metadata
		out.writeBoolean(this.moreData);// 1
		// V1
		if (version >= 1) {
			out.writeLong(this.executionDate.getTime());
		}

		long complete = new Date().getTime();
		if (logger.isDebugEnabled()) {
			logger.debug(("write RowCache complete in " + (complete - start) + "ms"));
		}
	}

	public byte[] serialize() throws IOException {
		ByteArrayOutputStream baout = new ByteArrayOutputStream();
		Output kout = new Output(baout);
		writeObject(kout);
		kout.close();
		byte[] res = baout.toByteArray();
		return res;
	}

	protected void readObject(Input in) throws IOException, ClassNotFoundException {
		if (logger.isDebugEnabled()) {
			logger.debug(("reading from cache "));
		}
		Kryo kryo = new Kryo();
		kryo.setRegistrationRequired(true);
		kryo.setReferences(false);

		this.rows = new ArrayList<RawRow>();
		this.colNames = new ArrayList<String>();
		this.colTypes = new ArrayList<Integer>();

		long start = new Date().getTime();

		// we need a different dictionary to check for first occurences
		HashMap<Integer, Object> tempDict = new HashMap<Integer, Object>();

		for (String className : this.registration.keySet()) {
			kryo.register(Class.forName(className), this.registration.get(className).intValue());
		}

		// READ nb columns
		int rowSize = in.readInt();

		// READ column names
		for (int i = 0; i < rowSize; i++)
			this.colNames.add(in.readString());

		// READ columns types
		for (int i = 0; i < rowSize; i++)
			this.colTypes.add(in.readInt());

		// READ data
		int count = 0;
		// int iter =0;
		while (in.readBoolean()) {
			Object[] rawrow = new Object[rowSize];
			for (int ia = 0; ia < rowSize; ia++) {
				int type = in.read();// 4
				switch (type) {
				case MEMBER_REFERENCE:
					int ref = in.readInt();// 5
					rawrow[ia] = tempDict.get(ref);
					break;
				case MEMBER_DEFINITION:
					String value = in.readString();// 6
					// String refString =
					// DimensionValuesDictionary.INSTANCE.getRef(value);
					if (tempDict.get(value) == null) {
						tempDict.put(new Integer(count), value);
						count++;
					} else {
						throw new IOException("invalid stream state");// SFA: we
						// should
						// not
						// get
						// there
					}
					rawrow[ia] = value;
					break;
				case MEMBER_VALUE:
					rawrow[ia] = kryo.readClassAndObject(in);// 7
					break;
				}
			}
			RawRow row = new RawRow(rawrow);
			this.addRow(row);
		}

		this.moreData = in.readBoolean();
		if (version >= 1) {
			this.executionDate = new Date(in.readLong());
		}

		long complete = new Date().getTime();
		if (logger.isDebugEnabled()) {
			logger.debug(
					("deserialized  complete in " + (complete - start) + "ms, with " + this.rows.size() + " rows"));
		}
	}

	public static RawMatrix deserialize(byte[] serializedMatrix) throws IOException, ClassNotFoundException {
		Input in = new Input(new ByteArrayInputStream(serializedMatrix));
		try {
			HashMap<String, Integer> registration = new HashMap<String, Integer>();
	
			// read header first
	
			// classes registration for kryo
			int nbClasses = in.readInt();
			for (int i = 0; i < nbClasses; i++) {
				String className = in.readString();
				int id = in.readInt();
				registration.put(className, id);
			}
	
			// get version
			int position = in.position(); // in case there is no version available;
	
			int version;
			int type;
			int check_version = in.readInt();
	
			if (check_version == -1) {
				// version>=1, read the version in the stream
				version = in.readInt();// read the version
				if (version < 2) {
					type = RedisCacheType.RAW_MATRIX.ordinal();
				} else {
					type = in.readInt();
				}
			} else {
				// version=0, no information in stream
				version = 0;
				type = RedisCacheType.RAW_MATRIX.ordinal();
				in.setPosition(position);
			}
	
			if (type == RedisCacheType.RAW_MATRIX.ordinal()) {
				RawMatrix res = new RawMatrix(version, registration);
				res.readObject(in);
				return res;
			} else {
				throw new ClassNotFoundException("Could not deserialize");
			}
		} finally {
			in.close();
		}
	}

	// JDBC type support -- need to find a better place in the JDBCDataFormatter
	// ?

	public static String getJavaDatatype(int colType) {

		switch (colType) {
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
			return "java.lang.String";

		case Types.NUMERIC:
		case Types.DECIMAL:
			return "java.math.BigDecimal";

		case Types.BIT:
			return "boolean";

		case Types.TINYINT:
			return "byte";

		case Types.SMALLINT:
			return "short";

		case Types.INTEGER:
			return "int";

		case Types.BIGINT:
			return "long";

		case Types.REAL:
			return "float";

		case Types.FLOAT:
		case Types.DOUBLE:
			return "double";

		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return "byte[]";

		case Types.DATE:
			return "java.sql.Date";

		case Types.TIME:
			return "java.sql.Time";

		case Types.TIMESTAMP:
			return "java.sql.Timestamp";

		default:
			return null;
		}
	}

	public static boolean isPrimitiveType(int colType) {

		switch (colType) {

		case Types.BIT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
		case Types.BIGINT:
		case Types.REAL:
		case Types.FLOAT:
		case Types.DOUBLE:
		case Types.BINARY:
		case Types.VARBINARY:
		case Types.LONGVARBINARY:
			return true;

		case Types.CHAR:
		case Types.VARCHAR:
		case Types.LONGVARCHAR:
		case Types.NUMERIC:
		case Types.DECIMAL:
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
			return false;

		default:
			return false;
		}
	}
	
	public static RawMatrix mergeMatrices(RawMatrix mergeInto, RawMatrix toMerge) throws ComputingException{
		if (mergeInto == null){
			return toMerge;
		}
		if (toMerge == null){
			return mergeInto ;
		}
		
		// check  if the  columns are the same (type +name) for both matrices
		if (! 	(mergeInto.getColTypes().equals(toMerge.getColTypes()) 
			&&  (mergeInto.getColNames().equals(toMerge.getColNames())))){ 
			throw new ComputingException("Trying to merge matrices with difference signature types");
		}
		mergeInto.getRows().addAll(toMerge.getRows());
		mergeInto.setExecutionDate(toMerge.getExecutionDate());
		mergeInto.setFromCache(toMerge.fromCache);
		mergeInto.setMoreData(toMerge.moreData);
		mergeInto.setRedisKey(null);
		return mergeInto;
	}
	
	
}
