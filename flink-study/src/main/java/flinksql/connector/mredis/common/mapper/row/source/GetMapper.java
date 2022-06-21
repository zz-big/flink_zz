/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinksql.connector.mredis.common.mapper.row.source;

import flinksql.connector.mredis.common.mapper.RedisCommand;
import org.apache.flink.configuration.ReadableConfig;

/** GET operation redis mapper. */
public class GetMapper extends RowRedisMapper {

    public GetMapper() {
        super(RedisCommand.GET);
    }

    public GetMapper(ReadableConfig readableConfig) {
        super(RedisCommand.GET);
    }
}
