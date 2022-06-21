/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flink.sql.sink.postgresql;


import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.postgresql.writer.CopyWriter;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.AbstractRdbSink;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import com.dtstack.flink.sql.sink.rdb.writer.AbstractUpsertWriter;
import com.dtstack.flink.sql.sink.rdb.writer.JDBCWriter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author maqi
 */
public class PostgresqlSink extends AbstractRdbSink implements IStreamSinkGener<AbstractRdbSink> {
    public PostgresqlSink() {
        super(new PostgresqlDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDbUrl(dbUrl)
                .setDialect(jdbcDialect)
                .setUsername(userName)
                .setPassword(password)
                .setTableName(tableName)
                .setSchema(schema)
                .build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(jdbcOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode)
                .setJDBCWriter(createJdbcWriter())
                .build();
    }
    private JDBCWriter createJdbcWriter(){
        if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.APPEND.name()) || primaryKeys == null || primaryKeys.size() == 0) {
            return new CopyWriter(tableName, fieldNames, null);
        }
        return AbstractUpsertWriter.create(
                jdbcDialect, schema, tableName, fieldNames, sqlTypes, primaryKeys.toArray(new String[primaryKeys.size()]), null,
                true, allReplace, null);
    }
}
