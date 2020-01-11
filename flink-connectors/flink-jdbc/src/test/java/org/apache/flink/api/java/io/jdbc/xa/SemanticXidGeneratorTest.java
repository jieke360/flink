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

package org.apache.flink.api.java.io.jdbc.xa;

import javax.transaction.xa.Xid;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static junit.framework.TestCase.assertEquals;
import static org.apache.flink.api.java.io.jdbc.xa.JdbcXaSinkTestBase.TEST_RUNTIME_CONTEXT;

/**
 * Simple uniqueness tests for the {@link SemanticXidGenerator}.
 */
public class SemanticXidGeneratorTest {
	private static final int COUNT = 10_000;

	public void testXidsUniqueAmongCheckpoints() {
		SemanticXidGenerator xidGenerator = new SemanticXidGenerator();
		checkUniqueness(checkpoint -> xidGenerator.generateXid(TEST_RUNTIME_CONTEXT, checkpoint));
	}

	public void testXidsUniqueAmongGenerators() {
		long checkpointId = 1L;
		checkUniqueness(unused -> new SemanticXidGenerator().generateXid(TEST_RUNTIME_CONTEXT, checkpointId));
	}

	private void checkUniqueness(Function<Integer, Xid> generate) {
		Set<Xid> generated = new HashSet<>();
		for (int i = 0; i < COUNT; i++) {
			generated.add(generate.apply(i));
		}
		assertEquals(COUNT, generated.size());
	}

}
