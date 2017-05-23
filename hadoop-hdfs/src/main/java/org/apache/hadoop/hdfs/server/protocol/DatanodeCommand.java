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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class DatanodeCommand implements Writable {
    static class Register extends DatanodeCommand {
        private Register() {
            super(DatanodeProtocol.DNA_REGISTER);
        }

        public void readFields(DataInput in) {
        }

        public void write(DataOutput out) {
        }
    }

    static class Finalize extends DatanodeCommand {
        private Finalize() {
            super(DatanodeProtocol.DNA_FINALIZE);
        }

        public void readFields(DataInput in) {
        }

        public void write(DataOutput out) {
        }
    }

    static {                                      // register a ctor
        WritableFactories.setFactory(Register.class, Register::new);
        WritableFactories.setFactory(Finalize.class, Finalize::new);
    }

    public static final DatanodeCommand REGISTER = new Register();
    public static final DatanodeCommand FINALIZE = new Finalize();

    private int action;

    public DatanodeCommand() {
        this(DatanodeProtocol.DNA_UNKNOWN);
    }

    DatanodeCommand(int action) {
        this.action = action;
    }

    public int getAction() {
        return this.action;
    }

    ///////////////////////////////////////////
    // Writable
    ///////////////////////////////////////////
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.action);
    }

    public void readFields(DataInput in) throws IOException {
        this.action = in.readInt();
    }
}