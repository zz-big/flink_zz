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

package com.dtstack.flink.sql.exception.sqlparse;

import com.dtstack.flink.sql.exception.ErrorCode;

/**
 * @author: chuixue
 * @create: 2020-11-30 16:56
 * @description:sql解析错误码
 **/
public enum SqlParseCodeEnum implements ErrorCode {
    /**
     * 流join维表时，select、join、group by等字段未使用t.field
     */
    WITHOUT_TABLENAME("001", "field invalid , please use like t.field"),
    /**
     * 在使用flink planner的时候，使用的是left join side s语法
     */
    PLANNER_NOT_MATCH("002", SqlExceptionConstant.plannerNotMatch()),
    /**
     * 在使用dtstack planner的时候，使用的是left join side FOR SYSTEM_TIME AS OF u.PROCTIME AS s语法
     */
    PLANNER_NOT_MATCH2("003", SqlExceptionConstant.plannerNotMatch2()),
    /**
     * 在使用flink planner的时候，create view中如果和维表关联必须使用别名，直接insert into则不会，flink原生bug
     */
    VIEW_JOIN_WITHOUT_ALIAS("004", SqlExceptionConstant.viewJoinWithoutAlias()),
    /**
     * PRIMARY  KEY中的字段未在表明中定义，如果表字段是别名，则以别名为准
     */
    FIELDS_NOT_FOUND_IN_TABLE("005", "PRIMARY  KEY field not found in table"),
    ;

    /**
     * 错误码
     */
    private final String code;

    /**
     * 描述
     */
    private final String description;

    /**
     * @param code        错误码
     * @param description 描述
     */
    private SqlParseCodeEnum(final String code, final String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
