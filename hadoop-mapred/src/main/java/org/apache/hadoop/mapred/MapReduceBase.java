/**
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

package org.apache.hadoop.mapred;

import java.io.Closeable;
import java.io.IOException;

/** 
 * Base class for {@link Mapper} and {@link Reducer} implementations.
 * 
 * <p>Provides default no-op implementations for a few methods, most non-trivial
 * applications need to override some of them.</p>
 */
public class MapReduceBase implements Closeable, JobConfigurable {

  /** Default implementation that does nothing. */
  public void close() throws IOException {
  }

  /** Default implementation that does nothing. */
  public void configure(JobConf job) {
  }

}