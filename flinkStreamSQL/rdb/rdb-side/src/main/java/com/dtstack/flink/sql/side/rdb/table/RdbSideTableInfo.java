/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.side.rdb.table;

import com.dtstack.flink.sql.core.rdb.JdbcCheckKeys;
import com.dtstack.flink.sql.core.rdb.JdbcResourceCheck;
import com.dtstack.flink.sql.resource.ResourceCheck;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class RdbSideTableInfo extends AbstractSideTableInfo {

    public static final String DRIVER_NAME = "driverName";
    public static final String URL_KEY = "url";
    public static final String TABLE_NAME_KEY = "tableName";
    public static final String USER_NAME_KEY = "userName";
    public static final String PASSWORD_KEY = "password";
    public static final String SCHEMA_KEY = "schema";
    private static final long serialVersionUID = -1L;
    private String driverName;
    private String url;
    private String tableName;
    private String userName;
    private String password;
    private String schema;

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "rdb of URL is required");
        Preconditions.checkNotNull(tableName, "rdb of tableName is required");
        Preconditions.checkNotNull(userName, "rdb of userName is required");
        Preconditions.checkNotNull(password, "rdb of password is required");
        Preconditions.checkArgument(getFieldList().size() == getFieldExtraInfoList().size(),
                "fields and fieldExtraInfoList attributes must be the same length");
        // 是否在client端快速检测表资源是否可用,这样在client能访问资源的情况下快速失败,不用提交到集群检测
        if (getFastCheck()) {
            JdbcResourceCheck.getInstance().checkResourceStatus(this.getCheckProperties());
        }
        return true;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    @Override
    public String toString() {
        String cacheInfo = super.toString();
        String connectionInfo = "Rdb Side Connection Info{" +
                "url='" + url + '\'' +
                ", tableName='" + tableName + '\'' +
                ", schema='" + schema + '\'' +
                ", driverName='" + driverName + '\'' +
                '}';
        return cacheInfo + " , " + connectionInfo;
    }

    @Override
    public Map<String, String> buildCheckProperties() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(JdbcCheckKeys.DRIVER_NAME, getDriverName());
        properties.put(JdbcCheckKeys.URL_KEY, getUrl());
        properties.put(JdbcCheckKeys.USER_NAME_KEY, getUserName());
        properties.put(JdbcCheckKeys.PASSWORD_KEY, getPassword());
        properties.put(JdbcCheckKeys.SCHEMA_KEY, getSchema());
        properties.put(JdbcCheckKeys.TABLE_NAME_KEY, getTableName());
        properties.put(JdbcCheckKeys.OPERATION_NAME_KEY, getName());
        properties.put(JdbcCheckKeys.TABLE_TYPE_KEY, "side");
        properties.put(JdbcCheckKeys.NEED_CHECK, String.valueOf(ResourceCheck.NEED_CHECK));
        return properties;
    }
}
