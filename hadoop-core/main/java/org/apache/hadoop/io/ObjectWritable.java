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

package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

/**
 * A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
public class ObjectWritable implements Writable, Configurable {

    private Class declaredClass;
    private Object instance;
    private Configuration conf;

    public ObjectWritable() {
    }

    public ObjectWritable(Object instance) {
        set(instance);
    }

    public ObjectWritable(Class declaredClass, Object instance) {
        this.declaredClass = declaredClass;
        this.instance = instance;
    }

    /**
     * Return the instance, or null if none.
     */
    public Object get() {
        return instance;
    }

    /**
     * Return the class this is meant to be.
     */
    public Class getDeclaredClass() {
        return declaredClass;
    }

    /**
     * Reset the instance.
     */
    public void set(Object instance) {
        this.declaredClass = instance.getClass();
        this.instance = instance;
    }

    public String toString() {
        return "OW[class=" + declaredClass + ",value=" + instance + "]";
    }


    public void readFields(DataInput in) throws IOException {
        readObject(in, this, this.conf);
    }

    public void write(DataOutput out) throws IOException {
        writeObject(out, instance, declaredClass, conf);
    }

    private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();

    static {
        PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
        PRIMITIVE_NAMES.put("byte", Byte.TYPE);
        PRIMITIVE_NAMES.put("char", Character.TYPE);
        PRIMITIVE_NAMES.put("short", Short.TYPE);
        PRIMITIVE_NAMES.put("int", Integer.TYPE);
        PRIMITIVE_NAMES.put("long", Long.TYPE);
        PRIMITIVE_NAMES.put("float", Float.TYPE);
        PRIMITIVE_NAMES.put("double", Double.TYPE);
        PRIMITIVE_NAMES.put("void", Void.TYPE);
    }

    private static class NullInstance extends Configured implements Writable {
        private Class<?> declaredClass;

        public NullInstance() {
            super(null);
        }

        public NullInstance(Class declaredClass, Configuration conf) {
            super(conf);
            this.declaredClass = declaredClass;
        }

        public void readFields(DataInput in) throws IOException {
            String className = Text.readString(in);
            declaredClass = PRIMITIVE_NAMES.get(className);
            if (declaredClass == null) {
                try {
                    declaredClass = getConf().getClassByName(className);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e.toString());
                }
            }
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, declaredClass.getName());
        }
    }

    /**
     * Write a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding.
     * 可以往DataOutput接口中写入各种Java对象
     * 1. 先输出对象的类名
     *
     */
    public static void writeObject(DataOutput out, Object instance,
                                   Class declaredClass,
                                   Configuration conf) throws IOException {

        if (instance == null) {                       // null
            instance = new NullInstance(declaredClass, conf);
            declaredClass = Writable.class;
        }
        /*先输出对象的类名*/
        Text.writeString(out, declaredClass.getName()); // always write declared

        /*针对 数组、字符串、基本类型、Enum和Writable类型分别做判断处理*/
        if (declaredClass.isArray()) {                // array
            int length = Array.getLength(instance);
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                writeObject(out, Array.get(instance, i),
                        declaredClass.getComponentType(), conf);
            }

        } else if (declaredClass == String.class) {   // String
            Text.writeString(out, (String) instance);

        } else if (declaredClass.isPrimitive()) {     // primitive type

            if (declaredClass == Boolean.TYPE) {        // boolean
                out.writeBoolean((Boolean) instance);
            } else if (declaredClass == Character.TYPE) { // char
                out.writeChar((Character) instance);
            } else if (declaredClass == Byte.TYPE) {    // byte
                out.writeByte((Byte) instance);
            } else if (declaredClass == Short.TYPE) {   // short
                out.writeShort((Short) instance);
            } else if (declaredClass == Integer.TYPE) { // int
                out.writeInt((Integer) instance);
            } else if (declaredClass == Long.TYPE) {    // long
                out.writeLong((Long) instance);
            } else if (declaredClass == Float.TYPE) {   // float
                out.writeFloat((Float) instance);
            } else if (declaredClass == Double.TYPE) {  // double
                out.writeDouble((Double) instance);
            } else if (declaredClass == Void.TYPE) {    // void
            } else {
                throw new IllegalArgumentException("Not a primitive: " + declaredClass);
            }
        } else if (declaredClass.isEnum()) {         // enum
            Text.writeString(out, ((Enum) instance).name());
        } else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
            Text.writeString(out, instance.getClass().getName());
            ((Writable) instance).write(out);

        } else {
            throw new IOException("Can't write: " + instance + " as " + declaredClass);
        }
    }


    /**
     * Read a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding.
     */
    public static Object readObject(DataInput in, Configuration conf)
            throws IOException {
        return readObject(in, null, conf);
    }

    /**
     * Read a {@link Writable}, {@link String}, primitive type, or an array of
     * the preceding.
     */
    @SuppressWarnings("unchecked")
    public static Object readObject(DataInput in, ObjectWritable objectWritable, Configuration conf)
            throws IOException {
        String className = Text.readString(in);
        Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
        if (declaredClass == null) {
            try {
                declaredClass = conf.getClassByName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("readObject can't find class " + className, e);
            }
        }

        Object instance;

        if (declaredClass.isPrimitive()) {            // primitive types

            if (declaredClass == Boolean.TYPE) {             // boolean
                instance = Boolean.valueOf(in.readBoolean());
            } else if (declaredClass == Character.TYPE) {    // char
                instance = Character.valueOf(in.readChar());
            } else if (declaredClass == Byte.TYPE) {         // byte
                instance = Byte.valueOf(in.readByte());
            } else if (declaredClass == Short.TYPE) {        // short
                instance = Short.valueOf(in.readShort());
            } else if (declaredClass == Integer.TYPE) {      // int
                instance = Integer.valueOf(in.readInt());
            } else if (declaredClass == Long.TYPE) {         // long
                instance = Long.valueOf(in.readLong());
            } else if (declaredClass == Float.TYPE) {        // float
                instance = Float.valueOf(in.readFloat());
            } else if (declaredClass == Double.TYPE) {       // double
                instance = Double.valueOf(in.readDouble());
            } else if (declaredClass == Void.TYPE) {         // void
                instance = null;
            } else {
                throw new IllegalArgumentException("Not a primitive: " + declaredClass);
            }

        } else if (declaredClass.isArray()) {              // array
            int length = in.readInt();
            instance = Array.newInstance(declaredClass.getComponentType(), length);
            for (int i = 0; i < length; i++) {
                Array.set(instance, i, readObject(in, conf));
            }

        } else if (declaredClass == String.class) {        // String
            instance = Text.readString(in);
        } else if (declaredClass.isEnum()) {         // enum
            instance = Enum.valueOf((Class<? extends Enum>) declaredClass, Text.readString(in));
        } else {                                      // Writable
            Class instanceClass = null;
            String str = "";
            try {
                str = Text.readString(in);
                instanceClass = conf.getClassByName(str);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("readObject can't find class " + str, e);
            }

            Writable writable = WritableFactories.newInstance(instanceClass, conf);
            writable.readFields(in);
            instance = writable;

            if (instanceClass == NullInstance.class) {  // null
                declaredClass = ((NullInstance) instance).declaredClass;
                instance = null;
            }
        }

        if (objectWritable != null) {                 // store values
            objectWritable.declaredClass = declaredClass;
            objectWritable.instance = instance;
        }

        return instance;

    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

}
