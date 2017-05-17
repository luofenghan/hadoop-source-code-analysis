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

package org.apache.hadoop.ipc;

import java.io.IOException;

/**
 * Superclass of all protocols that use Hadoop RPC.
 * Subclasses of this interface are also supposed to have
 * a static final long versionID field.
 */
public interface VersionedProtocol {

    /**
     * 返回协议接口的版本
     * 输入参数时协议接口的类名和客户端版本号，输出时服务器的协议接口版本号
     *
     * 在建立IPC时，getProtocolVersion 用于检查通信的双方，保证他们使用了相同的版本号
     * Return protocol version corresponding to protocol interface.
     * @param protocol 协议接口对应的接口名称
     * @param clientVersion 客户端期望的服务的版本号
     * @return the version that the server will speak
     */
    long getProtocolVersion(String protocol,
                            long clientVersion) throws IOException;
}
