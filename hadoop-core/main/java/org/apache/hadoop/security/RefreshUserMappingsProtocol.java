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
package org.apache.hadoop.security;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * Protocol use
 * Client（管理员）通过该协议更新用户——用户组映射关系
 */
@KerberosInfo(
        serverPrincipal = CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY)
public interface RefreshUserMappingsProtocol extends VersionedProtocol {

    /**
     * Version 1: Initial version.
     */
    long versionID = 1L;

    /**
     * Refresh user to group mappings.
     *
     * @throws IOException
     */
    void refreshUserToGroupsMappings() throws IOException;

    /**
     * Refresh superuser proxy group list
     *
     * @throws IOException
     */
    void refreshSuperUserGroupsConfiguration() throws IOException;
}
