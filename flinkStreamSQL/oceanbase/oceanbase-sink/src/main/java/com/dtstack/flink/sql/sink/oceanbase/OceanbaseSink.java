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
package com.dtstack.flink.sql.sink.oceanbase;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.AbstractRdbSink;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;

/**
 * @author : tiezhu
 * @date : 2020/3/24
 */
public class OceanbaseSink extends AbstractRdbSink implements IStreamSinkGener<AbstractRdbSink> {

    public OceanbaseSink() {
        super(new OceanbaseDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions oceanbaseOptions = JDBCOptions.builder()
                .setDbUrl(dbUrl)
                .setDialect(jdbcDialect)
                .setUsername(userName)
                .setPassword(password)
                .setTableName(tableName)
                .build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(oceanbaseOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode)
                .build();
    }
}
