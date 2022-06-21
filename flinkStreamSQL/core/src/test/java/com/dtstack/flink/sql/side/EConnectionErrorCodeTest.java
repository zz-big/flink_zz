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

package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.enums.EConnectionErrorCode;
import org.junit.Assert;
import org.junit.Test;

/**
 * Date: 2020/4/2
 * Company: www.dtstack.com
 * @author maqi
 */
public class EConnectionErrorCodeTest {

    @Test
    public void testResolveErrorCodeFromException(){
        EConnectionErrorCode errorCode =
                EConnectionErrorCode.resolveErrorCodeFromException(new Exception("The last packet successfully received from the server was 179 milliseconds"));

        EConnectionErrorCode ckSessionExpired =
                EConnectionErrorCode.resolveErrorCodeFromException(new Exception("Excepetion: Zookeeper session has been expired"));

        Assert.assertEquals(errorCode, EConnectionErrorCode.CONN_DB_INVALID);
        Assert.assertEquals(ckSessionExpired, EConnectionErrorCode.CONN_DB_INVALID);
    }
}
