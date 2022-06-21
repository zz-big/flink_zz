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

package com.dtstack.flink.sql;

import com.dtstack.flink.sql.exec.ApiResult;
import com.dtstack.flink.sql.exec.ExecuteProcessHelper;
import com.dtstack.flink.sql.exec.ParamsInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 *  local模式获取sql任务的执行计划
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class GetPlan {

    private static final Logger LOG = LoggerFactory.getLogger(GetPlan.class);

    public static String getExecutionPlan(String[] args) {
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            long start = System.currentTimeMillis();
            ParamsInfo paramsInfo = ExecuteProcessHelper.parseParams(args);
            paramsInfo.setGetPlan(true);

            ClassLoader envClassLoader = StreamExecutionEnvironment.class.getClassLoader();
            ClassLoader plannerClassLoader = URLClassLoader.newInstance(new URL[0], envClassLoader);
            Thread.currentThread().setContextClassLoader(plannerClassLoader);

            StreamTableEnvironment tableEnv = ExecuteProcessHelper.getStreamExecution(paramsInfo);
            StreamTableEnvironmentImpl tableEnvImpl = (StreamTableEnvironmentImpl) tableEnv;
            Field executionEnvironmentField = tableEnvImpl.getClass().getDeclaredField("executionEnvironment");
            executionEnvironmentField.setAccessible(true);
            StreamExecutionEnvironment env = (StreamExecutionEnvironment) executionEnvironmentField.get(tableEnvImpl);

            String executionPlan = env.getExecutionPlan();
            long end = System.currentTimeMillis();
            return ApiResult.createSuccessResultJsonStr(executionPlan, end - start);
        } catch (Exception e) {
            LOG.error("Get plan error", e);
            return ApiResult.createErrorResultJsonStr(ExceptionUtils.getFullStackTrace(e));
        } finally {
            Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }
}
