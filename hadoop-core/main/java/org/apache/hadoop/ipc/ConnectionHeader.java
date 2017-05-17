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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 当客户端与服务器间TCP连接建立后交换的第一条消息
 * 携带的内容包括ConnectionID中的用户信息和IPC接口信息，这两个域用于检查服务器是否实现了IPC接口，并确认客户端有权使用这个接口。
 *
 */
class ConnectionHeader implements Writable {
    public static final Log LOG = LogFactory.getLog(ConnectionHeader.class);

    private String protocol;
    private UserGroupInformation ugi = null;
    private AuthMethod authMethod;

    public ConnectionHeader() {
    }

    /**
     * Create a new {@link ConnectionHeader} with the given <code>protocol</code>
     * and {@link UserGroupInformation}.
     *
     * @param protocol protocol used for communication between the IPC client
     *                 and the server
     * @param ugi      {@link UserGroupInformation} of the client communicating with
     *                 the server
     */
    public ConnectionHeader(String protocol, UserGroupInformation ugi, AuthMethod authMethod) {
        this.protocol = protocol;
        this.ugi = ugi;
        this.authMethod = authMethod;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        protocol = Text.readString(in);
        if (protocol.isEmpty()) {
            protocol = null;
        }

        boolean ugiUsernamePresent = in.readBoolean();
        if (ugiUsernamePresent) {
            String username = in.readUTF();
            boolean realUserNamePresent = in.readBoolean();
            if (realUserNamePresent) {
                String realUserName = in.readUTF();
                UserGroupInformation realUserUgi = UserGroupInformation
                        .createRemoteUser(realUserName);
                ugi = UserGroupInformation.createProxyUser(username, realUserUgi);
            } else {
                ugi = UserGroupInformation.createRemoteUser(username);
            }
        } else {
            ugi = null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, (protocol == null) ? "" : protocol);
        if (ugi != null) {
            if (authMethod == AuthMethod.KERBEROS) {
                // Send effective user for Kerberos auth
                out.writeBoolean(true);
                out.writeUTF(ugi.getUserName());
                out.writeBoolean(false);
            } else if (authMethod == AuthMethod.DIGEST) {
                // Don't send user for token auth
                out.writeBoolean(false);
            } else {
                //Send both effective user and real user for simple auth
                out.writeBoolean(true);
                out.writeUTF(ugi.getUserName());
                if (ugi.getRealUser() != null) {
                    out.writeBoolean(true);
                    out.writeUTF(ugi.getRealUser().getUserName());
                } else {
                    out.writeBoolean(false);
                }
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public String getProtocol() {
        return protocol;
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public String toString() {
        return protocol + "-" + ugi;
    }
}
