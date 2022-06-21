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

package com.dtstack.flink.sql.side.rdb.async;

import org.apache.flink.api.java.typeutils.RowTypeInfo;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Reason:
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public class RdbAsyncSideInfo extends BaseSideInfo {

    private static final long serialVersionUID = 1942629132469918611L;
    private static final Logger LOG = LoggerFactory.getLogger(RdbAsyncSideInfo.class);

    public RdbAsyncSideInfo(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(sideTableInfo, lookupKeys);
    }

    public RdbAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();
        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        List<String> sqlJoinCompareOperate= Lists.newArrayList();

        ParseUtils.parseAnd(conditionNode, sqlNodeList);
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        sqlCondition = getSelectFromStatement(getTableName(rdbSideTableInfo), Arrays.asList(StringUtils.split(sideSelectFields, ",")),
                equalFieldList, sqlJoinCompareOperate, sideTableInfo.getPredicateInfoes());
        LOG.info(String.format("--------dimension sql query: %s-------\n", sqlCondition));
    }

    @Override
    public void buildEqualInfo(AbstractSideTableInfo sideTableInfo) {
        super.buildEqualInfo(sideTableInfo);
        RdbSideTableInfo rdbSideTableInfo = (RdbSideTableInfo) sideTableInfo;
        flinkPlannerSqlCondition = getSelectFromStatement(
                getTableName(rdbSideTableInfo)
                , new ArrayList(sideTableInfo.getPhysicalFields().values())
                , getRealLookupKeys(Arrays.asList(lookupKeys))
                , sideTableInfo.getPredicateInfoes());
        LOG.info(String.format("--------dimension sql query: %s-------\n", flinkPlannerSqlCondition));
    }

    public String getAdditionalWhereClause() {
        return "";
    }

    public String getSelectFromStatement(String tableName, List<String> selectFields, List<String> conditionFields, List<String> sqlJoinCompareOperate,
                                         List<PredicateInfo> predicateInfoes) {
        String fromClause = selectFields.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String whereClause = conditionFields.stream()
                .map(f -> quoteIdentifier(sideTableInfo.getPhysicalFields().getOrDefault(f, f)) + sqlJoinCompareOperate.get(conditionFields.indexOf(f)) + wrapperPlaceholder(f))
                .collect(Collectors.joining(" AND "));

        String predicateClause = predicateInfoes.stream()
                .map(this::buildFilterCondition)
                .collect(Collectors.joining(" AND "));

        String dimQuerySql = "SELECT " + fromClause + " FROM " + tableName + (conditionFields.size() > 0 ? " WHERE " + whereClause : "")
                + (predicateInfoes.size() > 0 ? " AND " + predicateClause : "") + getAdditionalWhereClause();

        return dimQuerySql;
    }

    /**
     * 构建异步维表的查询sql
     *
     * @param tableName       表名称
     * @param selectFields    选择字段
     * @param conditionFields 过滤字段
     * @param predicateInfoes 谓词信息
     * @return
     */
    public String getSelectFromStatement(String tableName, List<String> selectFields, List<String> conditionFields,
                                         List<PredicateInfo> predicateInfoes) {
        String fromClause = selectFields.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String whereClause = conditionFields.stream()
                .map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(" AND "));

        String predicateClause = predicateInfoes.stream()
                .map(this::buildFilterCondition)
                .collect(Collectors.joining(" AND "));

        String dimQuerySql = "SELECT " + fromClause + " FROM " + tableName + (conditionFields.size() > 0 ? " WHERE " + whereClause : "")
                + (predicateInfoes.size() > 0 ? " AND " + predicateClause : "") + getAdditionalWhereClause();

        return dimQuerySql;
    }

    public String wrapperPlaceholder(String fieldName) {
        return " ? ";
    }

    public String buildFilterCondition(PredicateInfo info) {
        switch (info.getOperatorKind()) {
            case "IN":
            case "NOT_IN":
                return quoteIdentifier(info.getFieldName()) + " " + info.getOperatorName() + " ( " + info.getCondition() + " )";
            case "NOT_EQUALS":
                return quoteIdentifier(info.getFieldName()) + " != " + info.getCondition();
            case "BETWEEN":
                return quoteIdentifier(info.getFieldName()) + " BETWEEN  " + info.getCondition();
            case "IS_NOT_NULL":
            case "IS_NULL":
                return quoteIdentifier(info.getFieldName()) + " " + info.getOperatorName();
            default:
                return quoteIdentifier(info.getFieldName()) + " " + info.getOperatorName() + " " + info.getCondition();
        }
    }

    public String getTableName(RdbSideTableInfo rdbSideTableInfo) {
        return rdbSideTableInfo.getTableName();
    }

    public String quoteIdentifier(String identifier) {
        return " " + identifier + " ";
    }

}