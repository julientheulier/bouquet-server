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

import com.squid.core.csv.CSVSettingsBean;
import com.squid.core.csv.CSVWriter;
import com.squid.core.export.IRawExportSource;
import com.squid.core.jdbc.engine.IExecutionItem;
import com.squid.core.jdbc.formatter.DataFormatter;
import com.squid.core.jdbc.formatter.IJDBCDataFormatter;
import com.squid.core.jdbc.vendor.IVendorSupport;
import com.squid.core.jdbc.vendor.VendorSupportRegistry;
import com.squid.kraken.v4.KrakenConfig;
import com.squid.kraken.v4.api.core.NotYetImplementedException;
import com.squid.kraken.v4.caching.awsredis.datastruct.RawMatrix;
import com.squid.kraken.v4.core.analysis.datamatrix.DataMatrix;
import com.squid.kraken.v4.model.DataTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Properties;

public class ExportSourceWriterKafka implements ExportSourceWriter {

    private static final Logger logger = LoggerFactory
            .getLogger(ExportSourceWriterKafka.class);

    public static final String KAFKA_URL = new String(KrakenConfig.getProperty("feature.spark.kafka_url", "spark:6667"));
    public static final String SCHEMA_URL = new String(KrakenConfig.getProperty("feature.spark.schema_url", "spark:8081"));

    private static final int SPLIT_SIZE = 0;
    KafkaProducer<Object, Object> producer;
    String topicName;
    private boolean computeStatistics = false;
    private boolean insertHeader = true;
    private boolean insertType = true;
    private int[] statistics = null;
    private int linesWritten = 0;


    public ExportSourceWriterKafka() {
        topicName = "bouquet";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://"+SCHEMA_URL);
        this.producer = new KafkaProducer<Object, Object>(props);
    }


    public ExportSourceWriterKafka(String topicName) {
        this();
        this.topicName = topicName;
    }

    public long write(IExecutionItem executionItem) {
        // TODO Auto-generated method stub
        throw new NotYetImplementedException();
    }

    public long write(RawMatrix matrix) {
        // TODO Auto-generated method stub
        throw new NotYetImplementedException();
    }

    private Object replaceNullString(int columnType, Object value) {
        if(value == null){
            switch(columnType) {
                case Types.BOOLEAN:
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    break;
                //long

                case Types.FLOAT:
                    break;

                //long
                case Types.BIGINT:
                    break;
                //double
                case Types.DECIMAL:
                case Types.REAL:
                case Types.NUMERIC:
                case Types.DOUBLE:
                    break;

                //bytes
                case Types.BINARY:
                case Types.BIT:
                    break;

                //string
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                case Types.CHAR:
                    value="";
                    break;

                //record
                //enum
                //array
                case Types.ARRAY:
                    break;
                //map
                //fixed

                // it is a string by default
                default:
                    value="";
                    break;
            }
        }
        return value;
    }

    public long write(DataMatrix matrix) {
        return write(matrix,producer);
    }



    public long write(DataMatrix matrix,  KafkaProducer<Object, Object> producer) {

    	IRawExportSource source = new DataMatrixExportSource(matrix);
        Schema schema = source.getSchema();
        GenericRecord avroRecord = new Record(schema);

        Iterator<Object[]> iterator = source.iterator();
        while (iterator.hasNext()) {
            Object[] values = iterator.next();


            ++linesWritten;
            String[] nextLine = new String[source.getNumberOfColumns()];
            String line = "";
            for (int i = 0; i < source.getNumberOfColumns(); i++) {
                Object value = values[i];

                // https://github.com/confluentinc/schema-registry/issues/272
                // I need to prevent null in String type for now...
                // with the current avro schema, the put will fail anyway (server side will not see the error).
                value=replaceNullString(source.getColumnType(i), value);
                avroRecord.put(source.getColumnName(i), value);

            }
            try {
                MessageDigest shaDigest = MessageDigest.getInstance("SHA-1");
                int partitionkey = ByteBuffer.wrap(shaDigest.digest(matrix.getRedisKey().getBytes())).getInt();
                avroRecord.put("userid", "1");//String.valueOf(partitionkey));
                ProducerRecord<Object, Object> producerRecord = new ProducerRecord<Object, Object>(topicName, "out", avroRecord);
                producer.send(producerRecord);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }

        return this.linesWritten;
    }

    @Override
    public long write(RawMatrix matrix, OutputStream out){
        return write(matrix);
    }

    @Override
    public long write(DataMatrix matrix, OutputStream out){
        return write(matrix);
    }

	@Override
	public long write(DataTable matrix, OutputStream out) {
        throw new UnsupportedOperationException();				
    }
	
	@Override
	public long write(ExecuteAnalysisResult item, OutputStream outputStream) {
        throw new NotYetImplementedException();
	}

}
