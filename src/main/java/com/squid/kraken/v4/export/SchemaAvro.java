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

import java.sql.Types;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Created by lrabiet on 07/01/16.
 */
public class SchemaAvro {

    public static Schema constructAvroSchema(String name, int[] columnTypes, String[] columnNames) {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(name).fields().name(columnNames[0]).type().nullable().stringType().noDefault();

        for (int i = 1; i < columnTypes.length; i++) {
            switch (columnTypes[i]) {
                case Types.BOOLEAN:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().booleanType().booleanDefault(false);
                    break;
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().intType().intDefault(0);
                    break;
                //long

                case Types.FLOAT:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().floatType().floatDefault(0);
                    break;

                //long
                case Types.BIGINT:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().longType().longDefault(0);
                    break;
                //double
                case Types.DECIMAL:
                case Types.REAL:
                case Types.NUMERIC:
                case Types.DOUBLE:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().doubleType().doubleDefault(0);
                    break;

                //bytes
                case Types.BINARY:
                case Types.BIT:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().bytesType().bytesDefault("0");
                    break;

                //string
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                case Types.CHAR:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().stringType().stringDefault("");
                    break;

                //record
                //enum
                //array
                case Types.ARRAY:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().nullable().intType().intDefault(0);
                    break;
                //map
                //fixed

                default:
                    fieldAssembler = fieldAssembler.name(columnNames[i]).type().stringType().stringDefault("");
                    break;

            }

        }
        //Partition ID
        fieldAssembler = fieldAssembler.name("userid").type().stringType().stringDefault("0");

        return fieldAssembler.endRecord();
    }

}
