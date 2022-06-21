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



package com.dtstack.flink.sql.util;

import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reason:
 * Date: 2018/6/27
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PluginUtil {

    private static String SP = File.separator;

    private static final String JAR_SUFFIX = ".jar";

    private static final String CLASS_PRE_STR = "com.dtstack.flink.sql";

    private static final Logger LOG = LoggerFactory.getLogger(PluginUtil.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static URL buildSourceAndSinkPathByLoadMode(String type, String suffix, String localSqlPluginPath, String remoteSqlPluginPath, String pluginLoadMode) throws Exception {
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            return getRemoteJarFilePath(type, suffix, remoteSqlPluginPath, localSqlPluginPath, pluginLoadMode);
        }
        return getLocalJarFilePath(type, suffix, localSqlPluginPath, pluginLoadMode);
    }

    public static URL buildSidePathByLoadMode(String type, String operator, String suffix, String localSqlPluginPath, String remoteSqlPluginPath, String pluginLoadMode) throws Exception {
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            return getRemoteSideJarFilePath(type, operator, suffix, remoteSqlPluginPath, localSqlPluginPath, pluginLoadMode);
        }
        return getLocalSideJarFilePath(type, operator, suffix, localSqlPluginPath, pluginLoadMode);
    }

    public static String getJarFileDirPath(String type, String sqlRootDir, String pluginLoadMode){
        String jarPath = sqlRootDir + SP + type;

        checkJarFileDirPath(sqlRootDir, jarPath, pluginLoadMode);

        return jarPath;
    }

    public static String getSideJarFileDirPath(String pluginType, String sideOperator, String tableType, String sqlRootDir, String pluginLoadMode) throws MalformedURLException {
        String dirName = sqlRootDir + SP + pluginType + sideOperator + tableType.toLowerCase();

        checkJarFileDirPath(sqlRootDir, dirName, pluginLoadMode);

        return dirName;
    }

    private static void checkJarFileDirPath(String sqlRootDir, String path, String pluginLoadMode) {

        if (sqlRootDir == null || sqlRootDir.isEmpty()){
            if (pluginLoadMode.equalsIgnoreCase(EPluginLoadMode.LOCALTEST.name())) {
                LOG.warn("be sure you are not in LocalTest mode, if not, check the sqlRootDir");
                return;
            }

            throw new RuntimeException("sqlPlugin is empty !");
        }

        File jarFile = new File(path);

        if(!jarFile.exists()){
            throw new RuntimeException(String.format("path %s not exists!!!", path));
        }
    }

    public static String getGenerClassName(String pluginTypeName, String type) throws IOException {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type);
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." + pluginTypeName + "." + pluginClassName;
    }

    public static String getSqlParserClassName(String pluginTypeName, String type){

        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type) +  "Parser";
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." +  pluginTypeName + ".table." + pluginClassName;
    }


    public static String getSqlSideClassName(String pluginTypeName, String type, String operatorType){
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + operatorType + "ReqRow";
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." +  pluginTypeName + "." + pluginClassName;
    }

    public static String getTableFunctionClassName(String pluginTypeName, String type) {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + "TableFunction";
        return CLASS_PRE_STR + "." + type.toLowerCase() + "." + pluginTypeName + ".table." + pluginClassName;
    }

    public static String getAsyncTableFunctionClassName(String pluginTypeName, String type) {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + "AsyncTableFunction";
        return CLASS_PRE_STR + "." + type.toLowerCase() + "." + pluginTypeName + ".table." + pluginClassName;
    }

    public static Map<String,Object> objectToMap(Object obj) throws Exception{
        return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
    }

    public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{
        return  objectMapper.readValue(jsonStr, clazz);
    }

    public static Properties stringToProperties(String str) throws IOException{
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)));
        return properties;
    }

    public static URL getRemoteJarFilePath(String pluginType, String tableType, String remoteSqlRootDir, String localSqlPluginPath, String pluginLoadMode) throws Exception {
        return buildFinalJarFilePath(pluginType, tableType, remoteSqlRootDir, localSqlPluginPath, pluginLoadMode);
    }

    public static URL getLocalJarFilePath(String pluginType, String tableType, String localSqlPluginPath, String pluginLoadMode) throws Exception {
        return buildFinalJarFilePath(pluginType, tableType, null, localSqlPluginPath, pluginLoadMode);
    }

    public static URL buildFinalJarFilePath(String pluginType, String tableType, String remoteSqlRootDir, String localSqlPluginPath, String pluginLoadMode) throws Exception {
        String dirName = pluginType + tableType.toLowerCase();
        String prefix = String.format("%s-%s", pluginType, tableType.toLowerCase());
        String jarPath = localSqlPluginPath + SP + dirName;
        String jarName = getCoreJarFileName(jarPath, prefix, pluginLoadMode);
        String sqlRootDir = remoteSqlRootDir == null ? localSqlPluginPath : remoteSqlRootDir;
        return new URL("file:" + sqlRootDir + SP + dirName + SP + jarName);
    }

    public static URL getRemoteSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir, String localSqlPluginPath, String pluginLoadMode) throws Exception {
        return buildFinalSideJarFilePath(pluginType, sideOperator, tableType, remoteSqlRootDir, localSqlPluginPath, pluginLoadMode);
    }

    public static URL getLocalSideJarFilePath(String pluginType, String sideOperator,  String tableType, String localSqlPluginPath, String pluginLoadMode) throws Exception {
        return buildFinalSideJarFilePath(pluginType, sideOperator, tableType, null, localSqlPluginPath, pluginLoadMode);
    }

    public static URL buildFinalSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir, String localSqlPluginPath, String pluginLoadMode) throws Exception {
        String dirName = pluginType + sideOperator + tableType.toLowerCase();
        String prefix = String.format("%s-%s-%s", pluginType, sideOperator, tableType.toLowerCase());
        String jarPath = localSqlPluginPath + SP + dirName;
        String jarName = getCoreJarFileName(jarPath, prefix, pluginLoadMode);
        String sqlRootDir = remoteSqlRootDir == null ? localSqlPluginPath : remoteSqlRootDir;
        return new URL("file:" + sqlRootDir + SP + dirName + SP + jarName);
    }

    public static String upperCaseFirstChar(String str){
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static URL[] getPluginJarUrls(String pluginDir) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        File dirFile = new File(pluginDir);

        if (pluginDir.contains("null")) {
            return urlList.toArray(new URL[0]);
        }

        if(!dirFile.exists() || !dirFile.isDirectory()){
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if(files == null || files.length == 0){
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for(File file : files){
            URL pluginJarUrl = file.toURI().toURL();
            urlList.add(pluginJarUrl);
        }

        return urlList.toArray(new URL[0]);
    }

    public static String getCoreJarFileName(String path, String prefix, String pluginLoadMode) throws Exception {
        String coreJarFileName = null;
        File pluginDir = new File(path);
        if (pluginDir.exists() && pluginDir.isDirectory()){
            File[] jarFiles = pluginDir.listFiles((dir, name) ->
                    name.toLowerCase().startsWith(prefix) && name.toLowerCase().endsWith(".jar"));

            if (jarFiles != null && jarFiles.length > 0){
                coreJarFileName = jarFiles[0].getName();
            }
        }

        if (StringUtils.isEmpty(coreJarFileName) && !pluginLoadMode.equalsIgnoreCase(EPluginLoadMode.LOCALTEST.name())){
            throw new Exception("Can not find core jar file in path:" + path);
        }

        return coreJarFileName;
    }
}
