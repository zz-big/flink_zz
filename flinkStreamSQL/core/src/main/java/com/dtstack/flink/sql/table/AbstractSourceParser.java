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

 

package com.dtstack.flink.sql.table;

import com.dtstack.flink.sql.util.MathUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/7/4
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class AbstractSourceParser extends AbstractTableParser {

    private static final String VIRTUAL_KEY = "virtualFieldKey";
    private static final String WATERMARK_KEY = "waterMarkKey";
    private static final String NOTNULL_KEY = "notNullKey";

    private static Pattern virtualFieldKeyPattern = Pattern.compile("(?i)^(\\S+\\([^\\)]+\\))\\s+AS\\s+(\\w+)$");
    private static Pattern waterMarkKeyPattern = Pattern.compile("(?i)^\\s*WATERMARK\\s+FOR\\s+(\\S+)\\s+AS\\s+withOffset\\(\\s*(\\S+)\\s*,\\s*(\\d+)\\s*\\)$");
    private static Pattern notNullKeyPattern = Pattern.compile("(?i)^(\\w+)\\s+(\\w+)\\s+NOT\\s+NULL?$");

    public AbstractSourceParser() {
        addParserHandler(VIRTUAL_KEY, virtualFieldKeyPattern, this::dealVirtualField);
        addParserHandler(WATERMARK_KEY, waterMarkKeyPattern, this::dealWaterMark);
        addParserHandler(NOTNULL_KEY, notNullKeyPattern, this::dealNotNull);
    }

    protected void dealVirtualField(Matcher matcher, AbstractTableInfo tableInfo){
        AbstractSourceTableInfo sourceTableInfo = (AbstractSourceTableInfo) tableInfo;
        String fieldName = matcher.group(2);
        String expression = matcher.group(1);
        sourceTableInfo.addVirtualField(fieldName, expression);
    }

    protected void dealWaterMark(Matcher matcher, AbstractTableInfo tableInfo){
        AbstractSourceTableInfo sourceTableInfo = (AbstractSourceTableInfo) tableInfo;
        String eventTimeField = matcher.group(1);
        //FIXME Temporarily resolve the second parameter row_time_field
        Integer offset = MathUtil.getIntegerVal(matcher.group(3));
        sourceTableInfo.setEventTimeField(eventTimeField);
        sourceTableInfo.setMaxOutOrderness(offset);
    }

    protected void dealNotNull(Matcher matcher, AbstractTableInfo tableInfo) {
        String fieldName = matcher.group(1);
        String fieldType = matcher.group(2);
        Class fieldClass= dbTypeConvertToJavaType(fieldType);
        AbstractTableInfo.FieldExtraInfo fieldExtraInfo = new AbstractTableInfo.FieldExtraInfo();
        fieldExtraInfo.setNotNull(true);

        tableInfo.addPhysicalMappings(fieldName, fieldName);
        tableInfo.addField(fieldName);
        tableInfo.addFieldClass(fieldClass);
        tableInfo.addFieldType(fieldType);
        tableInfo.addFieldExtraInfo(fieldExtraInfo);
    }

}
