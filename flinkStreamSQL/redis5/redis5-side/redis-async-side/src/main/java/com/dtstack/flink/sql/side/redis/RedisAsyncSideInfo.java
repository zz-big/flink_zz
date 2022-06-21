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

package com.dtstack.flink.sql.side.redis;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author yanxi
 */
public class RedisAsyncSideInfo extends BaseSideInfo {
    private static final long serialVersionUID = -4851348392924455039L;

    public RedisAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    public RedisAsyncSideInfo(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(sideTableInfo, lookupKeys);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }
    }

    @Override
    public void buildEqualInfo(AbstractSideTableInfo sideTableInfo) {

    }
}
